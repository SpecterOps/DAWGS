package translate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// optimizerFixedSuffixQuery exercises a bounded variable expansion followed by a selective three-edge suffix.
const optimizerFixedSuffixQuery = `
MATCH (root:ExpansionRoot)
WHERE root.root_key = 'root'
MATCH p1 = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
MATCH p2 = (root)-[:Expand*0..16]->()-[:OptionA|OptionB|OptionC]->(predicate:PredicateNode)-[:JoinSuffix]->(head)-[:HeadToBridge|HeadToAlternateBridge*1..16]->(:BridgeNode)-[:ReachTerminal]->(terminal)
WHERE predicate.eligible = true
AND predicate.requires_review = false
AND predicate.allows_direct = true
AND (predicate.version = 1 OR predicate.required_approvals = 0)
RETURN p1, p2
`

// optimizerSafetyKindMapper returns deterministic numeric IDs for the kinds used by optimizer-safety fixtures.
func optimizerSafetyKindMapper() *pgutil.InMemoryKindMapper {
	mapper := pgutil.NewInMemoryKindMapper()

	for _, kind := range graph.StringsToKinds([]string{
		"AllExtendedRights",
		"CertTemplate",
		"Domain",
		"SuffixEdgeOne",
		"SuffixNodeOne",
		"SuffixNodeOneFor",
		"GenericAll",
		"Group",
		"IssuedSignedBy",
		"MemberOf",
		"SuffixNodeTwo",
		"SuffixEdgeThree",
		"PublishedTo",
		"RootCA",
		"RootCAFor",
		"SuffixEdgeTwo",
		"AdminTo",
		"Computer",
		"Tag_Tier_Zero",
		"User",
		"ExpansionRoot",
		"ExpansionNode",
		"Expand",
		"SuffixHead",
		"EnterSuffix",
		"SuffixMiddle",
		"ContinueSuffix",
		"SuffixTerminal",
		"CompleteSuffix",
		"OptionA",
		"OptionB",
		"OptionC",
		"PredicateNode",
		"JoinSuffix",
		"HeadToBridge",
		"HeadToAlternateBridge",
		"BridgeNode",
		"ReachTerminal",
	}) {
		mapper.Put(kind)
	}

	return mapper
}

// optimizerSafetySQL translates cypherQuery and returns its rendered PostgreSQL text.
func optimizerSafetySQL(t *testing.T, cypherQuery string) string {
	t.Helper()

	translation := optimizerSafetyTranslation(t, cypherQuery)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	return strings.Join(strings.Fields(formattedQuery), " ")
}

// optimizerSafetyTranslation parses and translates cypherQuery with the optimizer-safety kind mapper.
func optimizerSafetyTranslation(t *testing.T, cypherQuery string) Result {
	t.Helper()

	return optimizerSafetyTranslationWithParameters(t, cypherQuery, nil)
}

// optimizerSafetyTranslationWithParameters parses and translates cypherQuery with the supplied parameter values.
func optimizerSafetyTranslationWithParameters(t *testing.T, cypherQuery string, parameters map[string]any) Result {
	t.Helper()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), parameters, DefaultGraphID)
	require.NoError(t, err)

	return translation
}

// requireOptimizationLowering requires name to appear among the lowerings applied during translation.
func requireOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.Lowerings {
		if lowering.Name == name {
			return
		}
	}

	require.Failf(t, "missing optimization lowering", "expected lowering %q in %#v", name, summary.Lowerings)
}

// requireNoOptimizationLowering requires name to be absent from applied lowering diagnostics.
func requireNoOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.Lowerings {
		require.NotEqualf(t, name, lowering.Name, "unexpected applied lowering %q in %#v", name, summary.Lowerings)
	}
}

// requirePlannedOptimizationLowering requires name to appear in the optimizer's planned lowerings.
func requirePlannedOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.PlannedLowerings {
		if lowering.Name == name {
			return
		}
	}

	require.Failf(t, "missing planned optimization lowering", "expected planned lowering %q in %#v", name, summary.PlannedLowerings)
}

// requireNoPlannedOptimizationLowering requires name to be absent from the optimizer's planned lowerings.
func requireNoPlannedOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.PlannedLowerings {
		require.NotEqualf(t, name, lowering.Name, "unexpected planned lowering %q in %#v", name, summary.PlannedLowerings)
	}
}

// requirePlanParameterContains requires at least one translated parameter value to contain expected.
func requirePlanParameterContains(t *testing.T, translation Result, expected string) {
	t.Helper()

	for _, parameter := range translation.Parameters {
		if planQuery, ok := parameter.(string); ok && strings.Contains(planQuery, expected) {
			return
		}
	}

	require.Failf(t, "missing plan parameter content", "expected a plan parameter to contain %q in %#v", expected, translation.Parameters)
}

// requireSkippedOptimizationLowering requires a skipped-lowering diagnostic with the expected name and reason.
func requireSkippedOptimizationLowering(t *testing.T, summary OptimizationSummary, name string, reason string) {
	t.Helper()

	for _, lowering := range summary.SkippedLowerings {
		if lowering.Name == name {
			require.Equal(t, reason, lowering.Reason)
			return
		}
	}

	require.Failf(t, "missing skipped optimization lowering", "expected skipped lowering %q in %#v", name, summary.SkippedLowerings)
}

// requireSkippedOptimizationLoweringCount requires a skipped-lowering diagnostic with the expected occurrence count.
func requireSkippedOptimizationLoweringCount(t *testing.T, summary OptimizationSummary, name string, count int) {
	t.Helper()

	for _, lowering := range summary.SkippedLowerings {
		if lowering.Name == name {
			require.Equal(t, count, lowering.Count)
			return
		}
	}

	require.Failf(t, "missing skipped optimization lowering", "expected skipped lowering %q in %#v", name, summary.SkippedLowerings)
}

// requireNoSkippedOptimizationLowering requires name to be absent from skipped-lowering diagnostics.
func requireNoSkippedOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.SkippedLowerings {
		require.NotEqualf(t, name, lowering.Name, "unexpected skipped lowering %q in %#v", name, summary.SkippedLowerings)
	}
}

func TestOptimizerSafetyReportsPartiallySkippedLowerings(t *testing.T) {
	t.Parallel()

	translator := NewTranslator(context.Background(), optimizerSafetyKindMapper(), nil, DefaultGraphID)
	translator.translation.Optimization.LoweringPlan = &optimize.LoweringPlan{
		PredicatePlacement: []optimize.PredicatePlacementDecision{
			{Target: optimize.TraversalStepTarget{StepIndex: 0}},
			{Target: optimize.TraversalStepTarget{StepIndex: 1}},
		},
	}

	translator.recordLowering(optimize.LoweringPredicatePlacement)
	translator.recordSkippedLowerings()

	requireOptimizationLowering(t, translator.translation.Optimization, optimize.LoweringPredicatePlacement)
	requireSkippedOptimizationLowering(t, translator.translation.Optimization, optimize.LoweringPredicatePlacement, "planned predicate placements were not consumed by this translation shape")
	requireSkippedOptimizationLoweringCount(t, translator.translation.Optimization, optimize.LoweringPredicatePlacement, 1)
}

// TestFixedSuffixSearchStrategyIsPlannedButConservativelySkipped verifies that an unforced candidate remains diagnostic-only.
func TestFixedSuffixSearchStrategyIsPlannedButConservativelySkipped(t *testing.T) {
	translation := optimizerSafetyTranslation(t, `
		MATCH (root:ExpansionRoot)
		WHERE root.root_key = $root_key
		MATCH path = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
		RETURN path
	`)

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy)
	requireNoOptimizationLowering(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy)
	requireSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy, optimize.ExpansionSearchFallbackTournamentUnqualified)
	require.Len(t, translation.Optimization.LoweringPlan.ExpansionSearchStrategy, 1)
	require.True(t, translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0].StructurallyEligible)
	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, "fixed_suffix_expansion", outcome.Family)
	require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV1), outcome.PlannedPolicy)
	require.Empty(t, outcome.EmittedPolicy)
	require.Equal(t, []string{"EXPANSION-STEPWISE-FORWARD", "EXPANSION-LATE-HYDRATED-FORWARD", "EXPANSION-FACTORED-SUFFIX-FORWARD", "EXPANSION-SUFFIX-SEEDED-REVERSE", "EXPANSION-BACKWARD-VIABILITY-FORWARD"}, outcome.PlannedCandidates)
	require.Equal(t, []string{string(optimize.ExpansionSearchStepwiseForward)}, outcome.EmittedCandidates)
	require.Equal(t, &optimize.ExpansionSearchProbeCaps{
		RootRowLimit:              optimize.ExpansionSearchOrientationRootRowLimit,
		ReverseSeedRowLimit:       optimize.ExpansionSearchOrientationReverseSeedRowLimit,
		DirectionalDegreeRowLimit: optimize.ExpansionSearchOrientationDirectionalDegreeRowLimit,
	}, outcome.ProbeCaps)
	require.Equal(t, &optimize.ExpansionSearchAdmission{
		StateLimit:             optimize.ExpansionSearchOrientationStateLimit,
		RequiresCompleteProbes: true,
		FallbackStrategy:       optimize.ExpansionSearchStepwiseForward,
	}, outcome.Admission)
	require.Contains(t, outcome.EligibilityFacts, TargetEligibilityFact{
		Name:     "qualified_fixed_suffix_topology",
		Eligible: true,
	})
	require.Equal(t, string(optimize.ExpansionSearchObservationFullPath), outcome.ObservationMode)
	require.NotNil(t, outcome.Eligible)
	require.True(t, *outcome.Eligible)
	require.Equal(t, "incumbent_default", outcome.SelectionMode)
	require.Equal(t, "fixed-suffix-static-v1", outcome.SelectorVersion)
	require.Equal(t, string(optimize.ExpansionSearchStepwiseForward), outcome.Selected)
	require.Equal(t, string(optimize.ExpansionSearchStepwiseForward), outcome.Fallback)
	require.Equal(t, optimize.ExpansionSearchFallbackTournamentUnqualified, outcome.SkipReason)
}

// TestForcedSuffixSeededReverseEmitsNativeReverseTrailState verifies the reverse-search CTE and ordered edge-ID state emitted by a forced strategy.
func TestForcedSuffixSeededReverseEmitsNativeReverseTrailState(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (root:ExpansionRoot)
		WHERE root.root_key = $root_key
		MATCH path = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
		RETURN path
	`)
	require.NoError(t, err)

	plan, err := optimize.Optimize(regularQuery)
	require.NoError(t, err)
	require.NoError(t, applyToolOptions(&plan, ToolOptions{
		ForceExpansionSearchStrategy: optimize.ExpansionSearchSuffixSeededReverse,
	}))
	require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 1)
	decision := plan.LoweringPlan.ExpansionSearchStrategy[0]
	require.Equal(t, optimize.ExpansionSearchSuffixSeededReverse, decision.SelectedStrategy)
	require.Empty(t, decision.EmittedPolicy)
	require.Equal(t, []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchSuffixSeededReverse}, decision.EmittedCandidates)
	require.Equal(t, "forced_tool", decision.SelectionMode)
	require.Equal(t, "suffix-seeded-reverse-tool-v1", decision.SelectorVersion)
	require.Empty(t, decision.FallbackReason)

	translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "forced-fixed-suffix-root",
	}, DefaultGraphID, ToolOptions{ForceExpansionSearchStrategy: optimize.ExpansionSearchSuffixSeededReverse})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "with recursive")
	require.Contains(t, formatted, "_suffix_seeded_suffix as materialized")
	require.Contains(t, formatted, "_suffix_seeded_reverse(boundary_id, next_id, depth, path)")
	require.Contains(t, formatted, "array_prepend(e0.id")
	require.Contains(t, formatted, "e0.id != all (s5_suffix_seeded_reverse.path)")
	require.Contains(t, formatted, "e0.end_id = s5_suffix_seeded_reverse.next_id")
	require.Contains(t, formatted, "s5_suffix_seeded_reverse.path && array [s5_suffix_seeded_suffix.e1, s5_suffix_seeded_suffix.e2, s5_suffix_seeded_suffix.e3]::int8[]")
	require.Contains(t, formatted, "e2.id != e1.id")
	require.Contains(t, formatted, "e3.id != e1.id")
	require.Contains(t, formatted, "e3.id != e2.id")
	require.NotContains(t, formatted, "s2(root_id, next_id, depth, satisfied, is_cycle, path)")

	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), outcome.Selected)
	require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), outcome.Applied)
	require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV1), outcome.PlannedPolicy)
	require.Empty(t, outcome.EmittedPolicy)
	require.Equal(t, []string{string(optimize.ExpansionSearchSuffixSeededReverse)}, outcome.EmittedCandidates)
	require.Equal(t, "forced_tool", outcome.SelectionMode)
	require.Empty(t, outcome.SkipReason)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy)
	requireNoSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy)
}

// TestEndpointSeededReverseIsAutomaticallyGuardedAndApplied verifies that qualified endpoint seeding emits bounded probes and reports application.
func TestEndpointSeededReverseIsAutomaticallyGuardedAndApplied(t *testing.T) {
	translation := optimizerSafetyTranslationWithParameters(t, `
		MATCH p = (c:Computer)-[:AdminTo]->(:User)-[:MemberOf*1..]->(g:Group)
		WHERE g.objectid ENDS WITH $suffix
		RETURN p
		LIMIT 1000
	`, map[string]any{"suffix": "-512"})

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "_endpoint_seeded_endpoints as materialized")
	require.Contains(t, formatted, "limit 33")
	require.Contains(t, formatted, "_endpoint_seeded_states as materialized")
	require.Contains(t, formatted, "_endpoint_seeded_incumbent as materialized")
	require.Contains(t, formatted, "limit 4097")
	require.Contains(t, formatted, "array_prepend")
	require.Contains(t, formatted, "_endpoint_seeded_reverse.next_id")
	require.Contains(t, formatted, "offset 32 limit 1")
	require.Contains(t, formatted, "offset 4096 limit 1")
	require.Contains(t, formatted, "_endpoint_seeded_incumbent")
	require.Contains(t, formatted, "_endpoint_seeded_states.path && array [")

	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy, optimize.TraversalStepTarget{
		QueryPartIndex: 0,
		ClauseIndex:    0,
		PatternIndex:   0,
		StepIndex:      1,
	})
	require.Equal(t, string(optimize.ExpansionSearchEndpointSeededReverse), outcome.Selected)
	require.Equal(t, string(optimize.ExpansionSearchEndpointSeededReverse), outcome.Applied)
	require.Equal(t, string(optimize.ExpansionSearchPolicyEndpointGuardV1), outcome.PlannedPolicy)
	require.Equal(t, string(optimize.ExpansionSearchPolicyEndpointGuardV1), outcome.EmittedPolicy)
	require.Equal(t, []string{string(optimize.ExpansionSearchStepwiseForward), string(optimize.ExpansionSearchEndpointSeededReverse)}, outcome.EmittedCandidates)
	require.Equal(t, &optimize.ExpansionSearchProbeCaps{ReverseSeedRowLimit: 32}, outcome.ProbeCaps)
	require.Equal(t, &optimize.ExpansionSearchAdmission{
		StateLimit:             4096,
		RequiresCompleteProbes: true,
		FallbackStrategy:       optimize.ExpansionSearchStepwiseForward,
	}, outcome.Admission)
	require.Equal(t, int64(32), outcome.EndpointLimit)
	require.Equal(t, int64(4096), outcome.StateLimit)
	require.Equal(t, "property_ends_with", outcome.SeedPredicateClass)
	require.Equal(t, 1, outcome.PrefixLength)
	require.True(t, outcome.HasFinalLimit)
}

func TestProductionEndpointSeededKillSwitchRestoresStepwiseSQL(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (c:Computer)-[:AdminTo]->(:User)-[:MemberOf*1..]->(g:Group)
		WHERE g.objectid ENDS WITH $suffix
		RETURN p LIMIT 1000
	`)
	require.NoError(t, err)
	translation, err := TranslateWithProductionOptions(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{"suffix": "-512"}, DefaultGraphID, ProductionOptions{
		DisableEndpointSeededReverse: true, SelectorVersion: "endpoint-seeded-kill-switch-v1",
	})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.NotContains(t, formatted, "_endpoint_seeded_endpoints")
	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy, optimize.TraversalStepTarget{QueryPartIndex: 0, ClauseIndex: 0, PatternIndex: 0, StepIndex: 1})
	require.Equal(t, string(optimize.ExpansionSearchStepwiseForward), outcome.Selected)
	require.Equal(t, "production_kill_switch", outcome.SelectionMode)
}

// TestOrdinaryExpansionMayContinueAfterSelfLoop verifies that encountering a self-loop does not stop unrelated recursive expansion.
func TestOrdinaryExpansionMayContinueAfterSelfLoop(t *testing.T) {
	formatted := optimizerSafetySQL(t, `MATCH p = (s)-[:MemberOf*1..3]->(g) RETURN p`)
	require.Contains(t, formatted, "1, false, false, array [e0.id]")
	require.NotContains(t, formatted, "e0.start_id = e0.end_id, array [e0.id]")
}

// TestForcedSuffixSeededReverseEndpointSQLIsParameterStable verifies deterministic parameter numbering in forced reverse-search SQL.
func TestForcedSuffixSeededReverseEndpointSQLIsParameterStable(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (root:ExpansionRoot)
		WHERE root.root_key = $root_key
		MATCH (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
		RETURN id(head), id(terminal)
	`)
	require.NoError(t, err)

	translateForced := func(rootKey string) string {
		translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
			"root_key": rootKey,
		}, DefaultGraphID, ToolOptions{ForceExpansionSearchStrategy: optimize.ExpansionSearchSuffixSeededReverse})
		require.NoError(t, err)
		formatted, err := Translated(translation)
		require.NoError(t, err)
		return formatted
	}

	first := translateForced("root-a")
	second := translateForced("root-b")
	require.Equal(t, first, second)
	require.Contains(t, first, "s5_suffix_seeded_reverse.path")
	require.Contains(t, first, "select s5.n2 as \"id(head)\", s5.n4 as \"id(terminal)\"")
	require.NotContains(t, first, "ordered_edge_ids_to_path")
	require.NotContains(t, first, "s2(root_id, next_id, depth, satisfied, is_cycle, path)")
}

// TestForcedSuffixSeededReversePreservesBoundaryConstraints verifies that predicates attached at the suffix boundary survive reversal.
func TestForcedSuffixSeededReversePreservesBoundaryConstraints(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (root:ExpansionRoot)
		WHERE root.root_key = $root_key
		MATCH (root)-[:Expand*0..16]->(boundary:ExpansionNode {enabled: true})-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
		RETURN id(head), id(terminal)
	`)
	require.NoError(t, err)

	translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "forced-fixed-suffix-root",
	}, DefaultGraphID, ToolOptions{ForceExpansionSearchStrategy: optimize.ExpansionSearchSuffixSeededReverse})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)

	require.Contains(t, formatted, "_suffix_seeded_suffix as materialized")
	require.Contains(t, formatted, "n1.kind_ids operator (pg_catalog.@>)")
	require.Contains(t, formatted, "n1.properties -> 'enabled'")
	require.Contains(t, formatted, "to_jsonb((true)::bool)")
}

// TestForcedFixedSuffixSearchRejectsUnsupportedStrategy verifies that tooling cannot force a strategy outside the candidate family.
func TestForcedFixedSuffixSearchRejectsUnsupportedStrategy(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
		RETURN id(head), id(terminal)
	`)
	require.NoError(t, err)

	_, err = TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), nil, DefaultGraphID, ToolOptions{
		ForceExpansionSearchStrategy: optimize.ExpansionSearchFactoredSuffixForward,
	})
	require.ErrorContains(t, err, "unsupported forced expansion-search strategy")
}

// TestForcedFixedSuffixSearchRejectsStructurallyIneligibleTarget verifies that forcing does not bypass structural qualification.
func TestForcedFixedSuffixSearchRejectsStructurallyIneligibleTarget(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)
		RETURN id(head)
	`)
	require.NoError(t, err)

	_, err = TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), nil, DefaultGraphID, ToolOptions{
		ForceExpansionSearchStrategy: optimize.ExpansionSearchSuffixSeededReverse,
	})
	require.ErrorContains(t, err, "has no structurally eligible target")
}

// TestForcedExpansionSearchRequiresExactlyOneEligibleTarget verifies that
// tooling fails closed before mutating any decision when a force is ambiguous.
func TestForcedExpansionSearchRequiresExactlyOneEligibleTarget(t *testing.T) {
	plan := optimize.Plan{LoweringPlan: optimize.LoweringPlan{
		ExpansionSearchStrategy: []optimize.ExpansionSearchStrategyDecision{
			{
				CandidateStrategy:    optimize.ExpansionSearchSuffixSeededReverse,
				SelectedStrategy:     optimize.ExpansionSearchStepwiseForward,
				StructurallyEligible: true,
			},
			{
				CandidateStrategy:    optimize.ExpansionSearchSuffixSeededReverse,
				SelectedStrategy:     optimize.ExpansionSearchStepwiseForward,
				StructurallyEligible: true,
			},
		},
	}}
	before := append([]optimize.ExpansionSearchStrategyDecision(nil), plan.LoweringPlan.ExpansionSearchStrategy...)

	err := applyForcedExpansionSearchStrategy(&plan, optimize.ExpansionSearchSuffixSeededReverse)
	require.ErrorContains(t, err, "matched 2 structurally eligible targets; expected exactly one")
	require.Equal(t, before, plan.LoweringPlan.ExpansionSearchStrategy)
}

// TestShortestDistanceExecutorIsAutomaticallySelectedAndReportedApplied verifies automatic scalar-distance selection and matching diagnostics.
func TestShortestDistanceExecutorIsAutomaticallySelectedAndReportedApplied(t *testing.T) {
	translation := optimizerSafetyTranslation(t, `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN length(p)
	`)

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringShortestPathExecutor)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringShortestPathExecutor)
	requireNoSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringShortestPathExecutor)
	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, "SP", outcome.Family)
	require.Equal(t, []string{"SP-S0", "SP-S0-DIRECT", "SP-S1", "SP-S2", "SP-S3-U-D", "SP-S3-U-E+MAT-M0", "SP-S4-C-D", "SP-S4-C-WE+MAT-M0", "SP-I1-C-D", "SP-I1-U-E+MAT-M0", "SP-I1-C-WE+MAT-M0", "SP-B1-C-ALT-NODE-D", "SP-B1-C-ALT-NODE-WE+MAT-M0", "SP-B2-C-MIN-LEVEL-D", "SP-B2-C-MIN-LEVEL-WE+MAT-M0"}, outcome.PlannedCandidates)
	require.Equal(t, string(optimize.ShortestPathSchedulerSingleEndedLevel), outcome.Scheduler)
	require.Contains(t, outcome.EligibilityFacts, TargetEligibilityFact{
		Name:     "one_static_id_equality_per_endpoint",
		Eligible: true,
	})
	require.Equal(t, string(optimize.ShortestPathObservationDistance), outcome.ObservationMode)
	require.NotNil(t, outcome.Eligible)
	require.True(t, *outcome.Eligible)
	require.Equal(t, "static", outcome.SelectionMode)
	require.Equal(t, "sp-static-v3", outcome.SelectorVersion)
	require.Equal(t, string(optimize.ShortestPathExecutorS3Unidirectional), outcome.Selected)
	require.Equal(t, string(optimize.ShortestPathExecutorS3Unidirectional), outcome.Applied)
	require.Equal(t, string(optimize.ShortestPathExecutorIncumbentWorkspace), outcome.Fallback)
	require.Empty(t, outcome.SkipReason)
}

// TestGreedyProjectionMaterializesShortestPathAndEntities verifies that RETURN * hydrates the path and every visible endpoint.
func TestGreedyProjectionMaterializesShortestPathAndEntities(t *testing.T) {
	translation := optimizerSafetyTranslation(t, `
		MATCH p = shortestPath((s:Group)-[:MemberOf*1..4]->(e:Group))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN *
	`)

	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, string(optimize.ShortestPathObservationOnePath), outcome.ObservationMode)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "::pathcomposite")
	require.Contains(t, formatted, "::nodecomposite")
}

// TestGreedyProjectionMaterializesRelationships verifies that RETURN * hydrates relationship bindings.
func TestGreedyProjectionMaterializesRelationships(t *testing.T) {
	formatted := optimizerSafetySQL(t, `
		MATCH (s:Group)-[r:MemberOf]->(e:Group)
		RETURN *
	`)

	require.Contains(t, formatted, "::nodecomposite")
	require.Contains(t, formatted, "::edgecomposite")
}

// TestGreedyWithProjectionCarriesFullShortestPath verifies that WITH * preserves a complete shortest-path value across query parts.
func TestGreedyWithProjectionCarriesFullShortestPath(t *testing.T) {
	translation := optimizerSafetyTranslation(t, `
		MATCH p = shortestPath((s:Group)-[:MemberOf*1..4]->(e:Group))
		WHERE id(s) = $start_id AND id(e) = $end_id
		WITH *
		RETURN p
	`)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "::pathcomposite")
	require.NotContains(t, formatted, "ordered_edge_ids_to_path")
	require.Contains(t, formatted, "m0_hydrated")
}

// TestShortestExecutorV4SelectsDeepInboundCompactDistance verifies canonical distance selection and inbound physical topology diagnostics.
func TestShortestExecutorV4SelectsDeepInboundCompactDistance(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((e)<-[:MemberOf*1..8]-(s))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN length(p)
	`)
	require.NoError(t, err)
	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID)
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "shortest_path_compact")
	require.NotContains(t, formatted, "sp_harness")
	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, "inbound", outcome.Direction)
	require.Equal(t, "end_id", outcome.PhysicalExpansion)
	require.Equal(t, 1, outcome.RelationshipKindCount)
	require.False(t, outcome.UntypedRelationship)
	require.Equal(t, "physical_inbound_deep", outcome.TopologyClassification)
	require.NotNil(t, outcome.Eligible)
	require.True(t, *outcome.Eligible)
	require.NotNil(t, outcome.StaticallyEligible)
	require.True(t, *outcome.StaticallyEligible)
	require.Equal(t, string(optimize.ShortestPathExecutorS4CanonicalDistance), outcome.Selected)
	require.Equal(t, string(optimize.ShortestPathExecutorS4CanonicalDistance), outcome.Applied)
	require.Empty(t, outcome.SkipReason)
}

// TestShortestExecutorV4SelectsCompactMultiKindPathAndKeepsS3Distance verifies observation-dependent selection for multi-kind paths.
func TestShortestExecutorV4SelectsCompactMultiKindPathAndKeepsS3Distance(t *testing.T) {
	for _, test := range []struct {
		// observation is the return expression that consumes the shortest path.
		observation string
		// selected is the executor expected for that observation.
		selected optimize.ShortestPathExecutor
		// reason is the expected translation skip reason, if any.
		reason string
	}{
		{
			observation: "p",
			selected:    optimize.ShortestPathExecutorS4CanonicalWitness,
		},
		{
			observation: "length(p)",
			selected:    optimize.ShortestPathExecutorS3Unidirectional,
		},
	} {
		regularQuery, err := frontend.ParseCypher(frontend.NewContext(), fmt.Sprintf(`
			MATCH p = shortestPath((s)-[:MemberOf|SuffixEdgeOne*1..8]->(e))
			WHERE id(s) = $start_id AND id(e) = $end_id
			RETURN %s
		`, test.observation))
		require.NoError(t, err)
		translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
			"start_id": int64(1), "end_id": int64(2),
		}, DefaultGraphID)
		require.NoError(t, err)
		formatted, err := Translated(translation)
		require.NoError(t, err)
		outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringShortestPathExecutor,
			optimize.TraversalStepTarget{
				QueryPartIndex: 0,
				ClauseIndex:    0,
				PatternIndex:   0,
				StepIndex:      0,
			})
		require.Equal(t, 2, outcome.RelationshipKindCount)
		require.Equal(t, string(test.selected), outcome.Selected)
		require.Equal(t, test.reason, outcome.SkipReason)
		if test.selected == optimize.ShortestPathExecutorS4CanonicalWitness {
			require.Contains(t, formatted, "generate_subscripts(s1.path, 1)")
			require.NotContains(t, formatted, "ordered_edge_ids_to_path")
		}
	}
}

// TestAllShortestDAGIsAutomaticallySelectedAndUsesTypedStaticExecutor verifies typed predecessor-DAG execution for bound all-shortest paths.
func TestAllShortestDAGIsAutomaticallySelectedAndUsesTypedStaticExecutor(t *testing.T) {
	translation := optimizerSafetyTranslationWithParameters(t, `
		MATCH p = allShortestPaths((s)-[*1..]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
	`, map[string]any{"start_id": int64(1), "end_id": int64(2)})

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "all_shortest_paths_dag")
	require.NotContains(t, formatted, "bidirectional_asp_harness")
	require.NotContains(t, formatted, "traversal_pair_filter")
	require.Contains(t, formatted, "array []::int2[]")

	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, "ASP", outcome.Family)
	require.Equal(t, []string{"SP-S0", "ASP-A1-DAG", "ASP-I1-U-DAG+MAT-M0", "ASP-B1-DAG-ALT-NODE", "ASP-B2-DAG-MIN-LEVEL"}, outcome.PlannedCandidates)
	require.Equal(t, string(optimize.ShortestPathSchedulerSingleEndedLevel), outcome.Scheduler)
	require.Equal(t, string(optimize.ShortestPathObservationAllPaths), outcome.ObservationMode)
	require.Equal(t, string(optimize.ShortestPathExecutorASPA1DAG), outcome.Selected)
	require.Equal(t, string(optimize.ShortestPathExecutorASPA1DAG), outcome.Applied)
	require.Equal(t, "asp-static-v1", outcome.SelectorVersion)
	require.Empty(t, outcome.SkipReason)
}

// TestForcedCompactBidirectionalExecutorsUseTypedKernels verifies every SP B1/B2
// identity reaches its scheduler wrapper without changing automatic selection.
func TestForcedCompactBidirectionalExecutorsUseTypedKernels(t *testing.T) {
	tests := []struct {
		executor     optimize.ShortestPathExecutor
		result       string
		functionName string
	}{
		{optimize.ShortestPathExecutorB1AlternatingNodeDistance, "length(p)", "shortest_path_b1_strict_alternating"},
		{optimize.ShortestPathExecutorB1AlternatingNodeWitness, "p", "shortest_path_b1_strict_alternating"},
		{optimize.ShortestPathExecutorB2SmallerCurrentLevelDistance, "length(p)", "shortest_path_b2_smaller_current_level"},
		{optimize.ShortestPathExecutorB2SmallerCurrentLevelWitness, "p", "shortest_path_b2_smaller_current_level"},
	}
	for _, test := range tests {
		t.Run(string(test.executor), func(t *testing.T) {
			regularQuery, err := frontend.ParseCypher(frontend.NewContext(), fmt.Sprintf(`
				MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE id(s) = $start_id AND id(e) = $end_id
				RETURN %s
			`, test.result))
			require.NoError(t, err)

			translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
				"start_id": int64(1), "end_id": int64(2),
			}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: test.executor})
			require.NoError(t, err)
			formatted, err := Translated(translation)
			require.NoError(t, err)
			require.Contains(t, formatted, test.functionName)
			require.Equal(t, 3, strings.Count(formatted, "100000"), formatted)
			require.NotContains(t, formatted, "bidirectional_sp_harness")

			outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringShortestPathExecutor,
				optimize.TraversalStepTarget{QueryPartIndex: 0, ClauseIndex: 0, PatternIndex: 0, StepIndex: 0})
			require.Equal(t, string(test.executor), outcome.Selected)
			require.Equal(t, string(test.executor), outcome.Applied)
			require.Equal(t, string(test.executor.Scheduler()), outcome.Scheduler)
			require.Equal(t, "forced_tool", outcome.SelectionMode)
		})
	}
}

// TestProductionCanaryShortestExecutorUsesVersionedSelectionMetadata verifies
// the production policy path emits the same qualified kernel while remaining
// distinguishable from tool forcing.
func TestProductionCanaryShortestExecutorUsesVersionedSelectionMetadata(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
	`)
	require.NoError(t, err)
	translation, err := TranslateWithProductionOptions(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ProductionOptions{
		ShortestPathExecutor: optimize.ShortestPathExecutorI1CanonicalPredecessorWitness,
		SelectorVersion:      "traversal-production-g7",
		ShortestPathCaps: &ProductionShortestPathCaps{
			StateLimit: 1000, PredecessorLimit: 1000, EnumerationLimit: 1000, OutputBytesLimit: 1 << 20,
		},
		AuthorizedBucket: &ProductionTraversalBucket{Direction: "outbound", ObservationMode: "one_path", MinimumDepth: 1, MaximumDepth: 4, RelationshipKindCount: 1},
	})
	require.NoError(t, err)
	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{QueryPartIndex: 0, ClauseIndex: 0, PatternIndex: 0, StepIndex: 0})
	require.Equal(t, "production_canary", outcome.SelectionMode)
	require.Equal(t, "traversal-production-g7", outcome.SelectorVersion)
	require.Equal(t, string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness), outcome.Applied)
}

func TestProductionRejectsToolOnlyBidirectionalShortestExecutor(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
	`)
	require.NoError(t, err)
	_, err = TranslateWithProductionOptions(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ProductionOptions{
		ShortestPathExecutor: optimize.ShortestPathExecutorB2SmallerCurrentLevelWitness,
		SelectorVersion:      "traversal-production-g7",
	})
	require.ErrorContains(t, err, "not production-canary eligible")
}

// TestForcedBidirectionalASPExecutorsUseTypedKernels verifies the tool-only
// candidates reach their two-sided predecessor-DAG wrappers while automatic
// production selection remains ASP-A1-DAG.
func TestForcedBidirectionalASPExecutorsUseTypedKernels(t *testing.T) {
	tests := []struct {
		executor     optimize.ShortestPathExecutor
		functionName string
	}{
		{optimize.ShortestPathExecutorASPB1AlternatingNodeDAG, "all_shortest_paths_b1_strict_alternating"},
		{optimize.ShortestPathExecutorASPB2SmallerCurrentLevelDAG, "all_shortest_paths_b2_smaller_current_level"},
	}
	for _, test := range tests {
		t.Run(string(test.executor), func(t *testing.T) {
			regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
				MATCH p = allShortestPaths((s)-[:MemberOf*1..4]->(e))
				WHERE id(s) = $start_id AND id(e) = $end_id
				RETURN p
			`)
			require.NoError(t, err)
			translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
				"start_id": int64(1), "end_id": int64(2),
			}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: test.executor})
			require.NoError(t, err)
			formatted, err := Translated(translation)
			require.NoError(t, err)
			require.Contains(t, formatted, test.functionName)
			require.NotContains(t, formatted, "bidirectional_asp_harness")
			require.Equal(t, 4, strings.Count(formatted, "100000"), formatted)
			require.Contains(t, formatted, "67108864")

			outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringShortestPathExecutor,
				optimize.TraversalStepTarget{QueryPartIndex: 0, ClauseIndex: 0, PatternIndex: 0, StepIndex: 0})
			require.Equal(t, string(test.executor), outcome.Selected)
			require.Equal(t, string(test.executor), outcome.Applied)
			require.Equal(t, string(test.executor.Scheduler()), outcome.Scheduler)
			require.Equal(t, "forced_tool", outcome.SelectionMode)
			require.Equal(t, "asp-tool-v1", outcome.SelectorVersion)
			require.Equal(t, string(optimize.ShortestPathExecutorASPA1DAG), outcome.Fallback)
			require.Equal(t, int64(100_000), outcome.EnumerationLimit)
			require.Equal(t, int64(64*1024*1024), outcome.OutputBytesLimit)
		})
	}
}

// TestForcedInlineASPExecutorUsesGuardedTypedStatement verifies the I1
// production-shaped emitter is forceable for qualification without changing
// the automatic ASP-A1 selection.
func TestForcedInlineASPExecutorUsesGuardedTypedStatement(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = allShortestPaths((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
	`)
	require.NoError(t, err)

	translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: optimize.ShortestPathExecutorASPI1DAG})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "asp_i1_distance")
	require.Contains(t, formatted, "asp_i1_direct")
	require.Contains(t, formatted, "asp_i1_predecessor_bounded")
	require.Contains(t, formatted, "asp_i1_paths_bounded")
	require.Contains(t, formatted, "asp_i1_admission")
	require.Contains(t, formatted, "asp_i1_candidate_marker")
	require.Contains(t, formatted, "asp_i1_fallback_marker")
	require.Contains(t, formatted, "all_shortest_paths_dag")
	require.Contains(t, formatted, "record_requested_traversal_runtime_attestation_v1")
	require.Contains(t, formatted, "record_requested_traversal_runtime_attestation_v1(case when asp_i1_admission.overflow")
	require.Contains(t, formatted, "end, asp_i1_admission.overflow, case when asp_i1_admission.overflow")
	require.Equal(t, 7, strings.Count(formatted, "offset 100000 limit 1"), formatted)
	require.Equal(t, 1, strings.Count(formatted, "67108864"), formatted)

	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{QueryPartIndex: 0, ClauseIndex: 0, PatternIndex: 0, StepIndex: 0})
	require.Equal(t, string(optimize.ShortestPathExecutorASPI1DAG), outcome.Selected)
	require.Equal(t, string(optimize.ShortestPathExecutorASPI1DAG), outcome.Applied)
	require.Equal(t, "guarded_dual_arm", outcome.ExecutionBoundary)
	require.Equal(t, string(optimize.ShortestPathExecutorASPA1DAG), outcome.Fallback)
	require.Equal(t, "forced_tool", outcome.SelectionMode)
	require.Equal(t, "asp-tool-v1", outcome.SelectorVersion)
}

func TestProductionInlineASPUsesAuthorizedBucketAndImmutableCaps(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = allShortestPaths((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
	`)
	require.NoError(t, err)
	options := ProductionOptions{
		ShortestPathExecutor: optimize.ShortestPathExecutorASPI1DAG,
		ShortestPathCaps: &ProductionShortestPathCaps{
			StateLimit: 31, PredecessorLimit: 37, EnumerationLimit: 41, OutputBytesLimit: 43000,
		},
		AuthorizedBucket: &ProductionTraversalBucket{
			Direction: "outbound", ObservationMode: "all_paths", MinimumDepth: 1, MaximumDepth: 4,
			RelationshipKindCount: 1, UntypedRelationship: false,
		},
		SelectorVersion: "asp-i1-canary-v1",
	}
	translation, err := TranslateWithProductionOptions(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, options)
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	for _, limit := range []string{"31", "37", "41", "43000"} {
		require.Contains(t, formatted, limit)
	}
	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{QueryPartIndex: 0, ClauseIndex: 0, PatternIndex: 0, StepIndex: 0})
	require.Equal(t, "production_canary", outcome.SelectionMode)
	require.Equal(t, "asp-i1-canary-v1", outcome.SelectorVersion)
	require.Equal(t, "asp-i1-guarded-v1", outcome.EmittedPolicy)
	require.Equal(t, []string{"ASP-I1-U-DAG+MAT-M0", "ASP-A1-DAG"}, outcome.EmittedCandidates)
	require.Equal(t, "guarded_dual_arm", outcome.ExecutionBoundary)
	require.Zero(t, outcome.FrontierLimit)

	options.AuthorizedBucket.MaximumDepth = 8
	_, err = TranslateWithProductionOptions(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, options)
	require.ErrorContains(t, err, "does not match its authorized promotion bucket")

	options.AuthorizedBucket = nil
	_, err = TranslateWithProductionOptions(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, options)
	require.ErrorContains(t, err, "requires an exact authorized bucket")
}

// TestForcedBidirectionalASPExecutorsFailClosedOutsideEnvelope verifies tool
// forcing cannot broaden the singleton, directed, predicate-free, read-only,
// minimum-depth-one all-path observation contract.
func TestForcedBidirectionalASPExecutorsFailClosedOutsideEnvelope(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "wrong observation", query: `MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p`},
		{name: "zero minimum", query: `MATCH p = allShortestPaths((s)-[:MemberOf*0..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p`},
		{name: "minimum two", query: `MATCH p = allShortestPaths((s)-[:MemberOf*2..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p`},
		{name: "maximum sixty five", query: `MATCH p = allShortestPaths((s)-[:MemberOf*1..65]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p`},
		{name: "directionless", query: `MATCH p = allShortestPaths((s)-[:MemberOf*1..4]-(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p`},
		{name: "path relationship predicate", query: `MATCH p = allShortestPaths((s)-[:MemberOf*1..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id AND all(r IN relationships(p) WHERE type(r) = 'MemberOf') RETURN p`},
		{name: "optional", query: `OPTIONAL MATCH p = allShortestPaths((s)-[:MemberOf*1..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p`},
		{name: "mutation", query: `MATCH p = allShortestPaths((s)-[:MemberOf*1..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id SET s.flag = true RETURN p`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			regularQuery, err := frontend.ParseCypher(frontend.NewContext(), test.query)
			require.NoError(t, err)
			for _, executor := range []optimize.ShortestPathExecutor{
				optimize.ShortestPathExecutorASPB1AlternatingNodeDAG,
				optimize.ShortestPathExecutorASPB2SmallerCurrentLevelDAG,
			} {
				_, err = TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
					"start_id": int64(1), "end_id": int64(2),
				}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: executor})
				require.ErrorContains(t, err, "no structurally eligible all-paths target")
			}
		})
	}
}

// TestForcedCompactBidirectionalExecutorsRejectUnsupportedDepth verifies the
// bounded maximum-depth envelope cannot be broadened by tool forcing.
func TestForcedCompactBidirectionalExecutorsRejectUnsupportedDepth(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..65]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN length(p)
	`)
	require.NoError(t, err)
	for _, executor := range []optimize.ShortestPathExecutor{
		optimize.ShortestPathExecutorB1AlternatingNodeDistance,
		optimize.ShortestPathExecutorB2SmallerCurrentLevelDistance,
	} {
		_, err = TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
			"start_id": int64(1), "end_id": int64(2),
		}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: executor})
		require.ErrorContains(t, err, "no structurally eligible distance-only target")
	}
}

// TestForcedShortestDistanceExecutorEmitsNativeScalarState verifies the scalar recursive state emitted by a forced distance executor.
func TestForcedShortestDistanceExecutorEmitsNativeScalarState(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN length(p)
	`)
	require.NoError(t, err)

	incumbent, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID)
	require.NoError(t, err)
	incumbentSQL, err := Translated(incumbent)
	require.NoError(t, err)
	productionOutcome := requireTraversalTargetOutcome(t, incumbent.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, string(optimize.ShortestPathExecutorS3Unidirectional), productionOutcome.Selected)
	require.Equal(t, string(optimize.ShortestPathExecutorS3Unidirectional), productionOutcome.Applied)
	require.Equal(t, "static", productionOutcome.SelectionMode)
	require.Equal(t, "sp-static-v3", productionOutcome.SelectorVersion)

	forced, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: optimize.ShortestPathExecutorS3Unidirectional})
	require.NoError(t, err)
	forcedSQL, err := Translated(forced)
	require.NoError(t, err)

	require.Equal(t, incumbentSQL, forcedSQL)
	require.Contains(t, forcedSQL, "with recursive")
	require.Contains(t, forcedSQL, "s1(next_id, depth)")
	require.NotContains(t, forcedSQL, "s1(root_id, next_id, depth)")
	require.Contains(t, forcedSQL, "select singleton_endpoints.root_id, 0 from singleton_endpoints")
	require.Contains(t, forcedSQL, "(select singleton_endpoints.root_id from singleton_endpoints) as n0")
	require.NotContains(t, forcedSQL, "sp_harness")
	require.NotContains(t, forcedSQL, "path)")
	require.NotContains(t, forcedSQL, "is_cycle")
	require.NotContains(t, forcedSQL, "cardinality")
	require.Contains(t, forcedSQL, "order by")
	require.Contains(t, forcedSQL, "depth limit 1")
	require.NotContains(t, forcedSQL, "join node")

	outcome := requireTraversalTargetOutcome(t, forced.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, string(optimize.ShortestPathExecutorS3Unidirectional), outcome.Selected)
	require.Equal(t, string(optimize.ShortestPathExecutorS3Unidirectional), outcome.Applied)
	require.Equal(t, "forced_tool", outcome.SelectionMode)
	require.Empty(t, outcome.SkipReason)
	requireOptimizationLowering(t, forced.Optimization, optimize.LoweringShortestPathExecutor)
	requireNoSkippedOptimizationLowering(t, forced.Optimization, optimize.LoweringShortestPathExecutor)
}

// TestForcedShortestIncumbentEmitsExactWorkspaceHarness verifies that forcing the incumbent preserves its workspace-table harness.
func TestForcedShortestIncumbentEmitsExactWorkspaceHarness(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN length(p)
	`)
	require.NoError(t, err)
	translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{
		ForceShortestPathExecutor: optimize.ShortestPathExecutorIncumbentWorkspace,
	})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "sp_harness")
	require.NotContains(t, formatted, "s1(next_id, depth)")
	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, string(optimize.ShortestPathExecutorIncumbentWorkspace), outcome.Selected)
	require.Equal(t, string(optimize.ShortestPathExecutorIncumbentWorkspace), outcome.Applied)
	require.Equal(t, "forced_tool", outcome.SelectionMode)
}

// TestForcedShortestDirectPreflightGatesWorkspaceFallback verifies that direct preflight gates the incumbent workspace branch.
func TestForcedShortestDirectPreflightGatesWorkspaceFallback(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((e)<-[:MemberOf|SuffixEdgeOne*1..8]-(s))
		WHERE id(e) = $end_id AND id(s) = $start_id
		RETURN p
	`)
	require.NoError(t, err)
	translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{
		ForceShortestPathExecutor: optimize.ShortestPathExecutorS0Direct,
	})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)

	require.Contains(t, formatted, "direct_shortest(root_id, next_id, depth, satisfied, is_cycle, path) as materialized")
	require.Contains(t, formatted, "fallback_endpoints as (select * from singleton_endpoints where not exists")
	require.Contains(t, formatted, "workspace_shortest(root_id, next_id, depth, satisfied, is_cycle, path)")
	require.Contains(t, formatted, "from fallback_endpoints, bidirectional_sp_harness")
	require.Contains(t, formatted, "select * from direct_shortest union all select * from workspace_shortest")

	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, string(optimize.ShortestPathExecutorS0Direct), outcome.Selected)
	require.Equal(t, string(optimize.ShortestPathExecutorS0Direct), outcome.Applied)
	require.Equal(t, "forced_tool", outcome.SelectionMode)
}

// TestForcedShortestDirectPreflightRejectsZeroMinimumDepth verifies that forcing cannot bypass the direct executor's positive-depth requirement.
func TestForcedShortestDirectPreflightRejectsZeroMinimumDepth(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*0..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
	`)
	require.NoError(t, err)

	_, err = TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{
		ForceShortestPathExecutor: optimize.ShortestPathExecutorS0Direct,
	})
	require.ErrorContains(t, err, "no structurally eligible depth-one target")
}

// TestForcedShortestDirectPreflightRejectsMutation verifies that statement mutation prevents direct shortest-path execution.
func TestForcedShortestDirectPreflightRejectsMutation(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		CREATE (:Group)
		RETURN p
	`)
	require.NoError(t, err)

	_, err = TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{
		ForceShortestPathExecutor: optimize.ShortestPathExecutorS0Direct,
	})
	require.ErrorContains(t, err, "no structurally eligible depth-one target")
}

// TestForcedShortestDirectPreflightPreservesPathThroughWithAlias verifies that a path witness survives aliasing across WITH.
func TestForcedShortestDirectPreflightPreservesPathThroughWithAlias(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		WITH p AS q
		RETURN q
	`)
	require.NoError(t, err)

	translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{
		ForceShortestPathExecutor: optimize.ShortestPathExecutorS0Direct,
	})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "direct_shortest")
	require.Contains(t, formatted, "ordered_edge_ids_to_path")
	require.Contains(t, formatted, "as q")
}

// TestForcedShortestDistanceExecutorRejectsIneligibleObservation verifies that a path consumer cannot force distance-only execution.
func TestForcedShortestDistanceExecutorRejectsIneligibleObservation(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
	`)
	require.NoError(t, err)

	_, err = TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: optimize.ShortestPathExecutorS3Unidirectional})
	require.ErrorContains(t, err, "no structurally eligible distance-only target")
}

// TestForcedShortestPathEdgeM0ExecutorEmitsNativeEdgeTrailAndMaterializer verifies ordered edge-trail state and deferred path hydration.
func TestForcedShortestPathEdgeM0ExecutorEmitsNativeEdgeTrailAndMaterializer(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
	`)
	require.NoError(t, err)

	incumbent, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID)
	require.NoError(t, err)
	incumbentSQL, err := Translated(incumbent)
	require.NoError(t, err)
	productionOutcome := requireTraversalTargetOutcome(t, incumbent.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, string(optimize.ShortestPathExecutorS3EdgeM0), productionOutcome.Selected)
	require.Equal(t, string(optimize.ShortestPathExecutorS3EdgeM0), productionOutcome.Applied)
	require.Equal(t, "static", productionOutcome.SelectionMode)
	require.Equal(t, "sp-static-v5-contained", productionOutcome.SelectorVersion)
	require.NotContains(t, incumbentSQL, "shortest_path_compact")

	forced, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: optimize.ShortestPathExecutorS3EdgeM0})
	require.NoError(t, err)
	forcedSQL, err := Translated(forced)
	require.NoError(t, err)

	require.Equal(t, incumbentSQL, forcedSQL, "forcing the contained S3 winner must reproduce the default SQL")
	require.Contains(t, forcedSQL, "with recursive")
	require.Contains(t, forcedSQL, "s1(next_id, depth, path)")
	require.Contains(t, forcedSQL, "generate_subscripts(s1.path, 1)")
	require.Equal(t, 1, strings.Count(forcedSQL, "generate_subscripts(s1.path, 1)"), forcedSQL)
	require.Contains(t, forcedSQL, "array_agg((m0_terminal.id, m0_terminal.kind_ids, m0_terminal.properties)::nodecomposite order by m0_path_index)")
	require.Contains(t, forcedSQL, "m0_hydrated.hydrated_count = cardinality(s1.path)")
	require.Contains(t, forcedSQL, "m0_terminal.id = m0_edge.end_id")
	require.Contains(t, forcedSQL, "::pathcomposite")
	require.NotContains(t, forcedSQL, "sp_harness")
	require.NotContains(t, forcedSQL, "ordered_edge_ids_to_path")

	outcome := requireTraversalTargetOutcome(t, forced.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, string(optimize.ShortestPathExecutorS3EdgeM0), outcome.Selected)
	require.Equal(t, string(optimize.ShortestPathExecutorS3EdgeM0), outcome.Applied)
	require.Equal(t, "forced_tool", outcome.SelectionMode)
	require.Empty(t, outcome.SkipReason)
}

// TestForcedShortestPathEdgeM0ExecutorIsDirectionAware verifies that edge-trail recursion joins the correct physical endpoint for each direction.
func TestForcedShortestPathEdgeM0ExecutorIsDirectionAware(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((e)<-[:MemberOf*1..8]-(s))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
	`)
	require.NoError(t, err)

	forced, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: optimize.ShortestPathExecutorS3EdgeM0})
	require.NoError(t, err)
	forcedSQL, err := Translated(forced)
	require.NoError(t, err)

	require.Contains(t, forcedSQL, "join edge e0 on e0.end_id = s1.next_id")
	require.Contains(t, forcedSQL, "m0_terminal.id = m0_edge.start_id")
}

// TestForcedShortestPathExecutorsRejectMismatchedObservation verifies tool
// forcing cannot broaden distance and witness observation contracts.
func TestForcedShortestPathExecutorsRejectMismatchedObservation(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN length(p)
	`)
	require.NoError(t, err)

	_, err = TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: optimize.ShortestPathExecutorS3EdgeM0})
	require.ErrorContains(t, err, "no structurally eligible one-path target")

	for _, executor := range []optimize.ShortestPathExecutor{
		optimize.ShortestPathExecutorB1AlternatingNodeWitness,
		optimize.ShortestPathExecutorB2SmallerCurrentLevelWitness,
	} {
		_, err = TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
			"start_id": int64(1), "end_id": int64(2),
		}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: executor})
		require.ErrorContains(t, err, "no structurally eligible one-path target")
	}

	witnessQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
	`)
	require.NoError(t, err)
	for _, executor := range []optimize.ShortestPathExecutor{
		optimize.ShortestPathExecutorB1AlternatingNodeDistance,
		optimize.ShortestPathExecutorB2SmallerCurrentLevelDistance,
	} {
		_, err = TranslateForTool(context.Background(), witnessQuery, optimizerSafetyKindMapper(), map[string]any{
			"start_id": int64(1), "end_id": int64(2),
		}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: executor})
		require.ErrorContains(t, err, "no structurally eligible distance-only target")
	}
}

// TestForcedShortestPathEdgeM0ExecutorPreservesPathThroughWithAlias verifies that a materialized witness survives aliasing across WITH.
func TestForcedShortestPathEdgeM0ExecutorPreservesPathThroughWithAlias(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		WITH p AS q
		RETURN q
	`)
	require.NoError(t, err)

	forced, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: optimize.ShortestPathExecutorS3EdgeM0})
	require.NoError(t, err)
	forcedSQL, err := Translated(forced)
	require.NoError(t, err)

	require.Contains(t, forcedSQL, "::pathcomposite")
	require.Contains(t, forcedSQL, "as q")
	require.NotContains(t, forcedSQL, "ordered_edge_ids_to_path")

	outcome := requireTraversalTargetOutcome(t, forced.Optimization, optimize.LoweringShortestPathExecutor,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, string(optimize.ShortestPathExecutorS3EdgeM0), outcome.Applied)
}

// TestForcedShortestDistanceExecutorIsDirectionAwareAndParameterStable verifies physical direction and deterministic parameters for scalar search.
func TestForcedShortestDistanceExecutorIsDirectionAwareAndParameterStable(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((e)<-[:MemberOf*1..8]-(s))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN length(p)
	`)
	require.NoError(t, err)

	translateForced := func(startID, endID int64) string {
		translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
			"start_id": startID, "end_id": endID,
		}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: optimize.ShortestPathExecutorS3Unidirectional})
		require.NoError(t, err)
		formatted, err := Translated(translation)
		require.NoError(t, err)
		return formatted
	}

	firstSQL := translateForced(1, 2)
	secondSQL := translateForced(100, 200)
	require.Equal(t, firstSQL, secondSQL)
	require.Contains(t, firstSQL, "select e0.start_id, s1.depth + 1")
	require.NotContains(t, firstSQL, "select s1.root_id, e0.start_id, s1.depth + 1")
	require.Contains(t, firstSQL, "join edge e0 on e0.end_id = s1.next_id")
}

// TestForcedShortestDistanceExecutorSupportsZeroDepthWithoutSelfEndpointError verifies legal same-endpoint zero-length paths.
func TestForcedShortestDistanceExecutorSupportsZeroDepthWithoutSelfEndpointError(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*0..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN length(p) AS distance
	`)
	require.NoError(t, err)

	translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(1),
	}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: optimize.ShortestPathExecutorS3Unidirectional})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)

	require.Contains(t, formatted, "s1.depth >= 0")
	require.NotContains(t, formatted, "shortest_path_self_endpoint_error")
	require.Contains(t, formatted, "(s0.ep0)::int as distance")
}

func TestProductionRejectsUnderGuardedInlineDistanceExecutor(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..8]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN length(p)
	`)
	require.NoError(t, err)
	_, err = TranslateWithProductionOptions(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ProductionOptions{ShortestPathExecutor: optimize.ShortestPathExecutorI1CanonicalDistance, SelectorVersion: "sp-i1-canary-v1"})
	require.ErrorContains(t, err, "not production-canary eligible")
}

func TestProductionInlineWitnessExecutorKeepsEdgeIDsAtMaterializationBoundary(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..8]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
	`)
	require.NoError(t, err)
	translation, err := TranslateWithProductionOptions(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ProductionOptions{
		ShortestPathExecutor: optimize.ShortestPathExecutorI1CanonicalPredecessorWitness,
		SelectorVersion:      "sp-i1-witness-canary-v1",
		ShortestPathCaps: &ProductionShortestPathCaps{
			StateLimit: 1000, PredecessorLimit: 1000, EnumerationLimit: 1000, OutputBytesLimit: 1 << 20,
		},
		AuthorizedBucket: &ProductionTraversalBucket{Direction: "outbound", ObservationMode: "one_path", MinimumDepth: 1, MaximumDepth: 8, RelationshipKindCount: 1},
	})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "with recursive")
	require.Contains(t, formatted, "generate_subscripts(s1.path, 1)")
	require.NotContains(t, formatted, "ordered_edge_ids_to_path")
	require.Contains(t, formatted, "shortest_path_compact")
	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringShortestPathExecutor, optimize.TraversalStepTarget{})
	require.Equal(t, string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness), outcome.Applied)
	require.Equal(t, "guarded_dual_arm", outcome.ExecutionBoundary)
	require.Equal(t, "production_canary", outcome.SelectionMode)
}

// TestForcedShortestDistanceExecutorPreservesDistanceThroughWithAlias verifies that scalar distance survives aliasing across WITH.
func TestForcedShortestDistanceExecutorPreservesDistanceThroughWithAlias(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		WITH length(p) AS distance
		RETURN distance
	`)
	require.NoError(t, err)

	translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: optimize.ShortestPathExecutorS3Unidirectional})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)

	require.NotContains(t, formatted, "cardinality")
	require.NotContains(t, formatted, "ordered_edge_ids_to_path")
	require.Contains(t, formatted, "::int as i0")
	require.Contains(t, formatted, "s0.i0 as distance")
}

// requireTraversalTargetOutcome returns the diagnostic outcome for one lowering and traversal target.
func requireTraversalTargetOutcome(t *testing.T, summary OptimizationSummary, lowering string, target optimize.TraversalStepTarget) TargetLoweringOutcome {
	t.Helper()

	for _, outcome := range summary.TargetOutcomes {
		if outcome.Lowering == lowering && outcome.TraversalTarget != nil && *outcome.TraversalTarget == target {
			return outcome
		}
	}

	require.FailNowf(t, "missing target outcome", "lowering %s target %+v", lowering, target)
	return TargetLoweringOutcome{}
}

func TestTraversalEnvelopeAnalysisHasExplicitTargetOutcomes(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) IN [1, 2] AND id(e) = 3
		  AND all(n IN nodes(p) WHERE n.enabled = true)
		RETURN p
	`)
	target := optimize.PatternTarget{QueryPartIndex: 0, ClauseIndex: 0, PatternIndex: 0}.TraversalStep(0)

	endpoint := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringEndpointResolution, target)
	require.Equal(t, "endpoint_resolution", endpoint.TargetKind)
	require.Equal(t, "endpoint_resolution", endpoint.Family)
	require.Equal(t, "SP", endpoint.TraversalFamily)
	require.Equal(t, string(optimize.EndpointResolutionPlanBounded), endpoint.Candidate)
	require.Equal(t, string(optimize.EndpointResolutionPlanIncumbent), endpoint.Selected)
	require.Equal(t, endpoint.Selected, endpoint.Applied)
	require.Equal(t, "analysis_only", endpoint.SelectionMode)
	require.Equal(t, optimize.EndpointResolutionFallbackPlannedOnly, endpoint.SkipReason)
	require.NotNil(t, endpoint.EndpointRoot)
	require.Equal(t, optimize.EndpointResolutionClassExplicitSmallSet, endpoint.EndpointRoot.Class)
	require.Equal(t, 2, endpoint.EndpointRoot.StaticValueCount)
	require.NotNil(t, endpoint.EndpointTerminal)
	require.Equal(t, optimize.EndpointResolutionClassIDEquality, endpoint.EndpointTerminal.Class)
	require.Equal(t, &optimize.EndpointResolutionCaps{
		SingletonLimit:    optimize.EndpointResolutionSingletonLimit,
		SingletonSentinel: optimize.EndpointResolutionSingletonSentinel,
		SmallSetLimit:     optimize.EndpointResolutionSmallSetLimit,
		SmallSetSentinel:  optimize.EndpointResolutionSmallSetSentinel,
	}, endpoint.EndpointResolutionCaps)

	var predicate *TargetLoweringOutcome
	for index := range translation.Optimization.TargetOutcomes {
		outcome := &translation.Optimization.TargetOutcomes[index]
		if outcome.Lowering == optimize.LoweringTraversalPredicateClassification && outcome.PredicateClass == optimize.TraversalPredicateClassUniversalAllNodes {
			predicate = outcome
			break
		}
	}
	require.NotNil(t, predicate)
	require.Equal(t, "traversal_predicate", predicate.TargetKind)
	require.Equal(t, string(optimize.TraversalPredicatePlanStep), predicate.Candidate)
	require.Equal(t, string(optimize.TraversalPredicatePlanIncumbent), predicate.Selected)
	require.Equal(t, predicate.Selected, predicate.Applied)
	require.Equal(t, optimize.TraversalPredicateFallbackPlannedOnly, predicate.SkipReason)
	require.Equal(t, "analysis_only", predicate.SelectionMode)
	require.NotNil(t, predicate.PredicateIndex)
}

// requireSQLContainsInOrder requires each SQL fragment to occur after the preceding fragment.
func requireSQLContainsInOrder(t *testing.T, sql string, parts ...string) {
	t.Helper()

	offset := 0
	for _, part := range parts {
		nextIndex := strings.Index(sql[offset:], part)
		require.NotEqualf(t, -1, nextIndex, "expected SQL to contain %q after offset %d:\n%s", part, offset, sql)
		offset += nextIndex + len(part)
	}
}

// TestOptimizerSafetyCountStoreFastPathUsesBaseNodeCount verifies unconstrained node counts use the graph-wide node count source.
func TestOptimizerSafetyCountStoreFastPathUsesBaseNodeCount(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH (n) RETURN count(n)`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringFieldRequirements, "analysis_metadata_only")
	require.Equal(t, "select count(*)::int8 from node n0;", strings.Join(strings.Fields(formattedQuery), " "))
}

func TestOptimizerSafetyCountStoreFastPathKeepsKindConstraintAndAlias(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH (n:Group) RETURN count(n) AS total`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	require.Equal(t, "select count(*)::int8 as total from node n0 where n0.kind_ids operator (pg_catalog.@>) array [8]::int2[];", strings.Join(strings.Fields(formattedQuery), " "))
}

func TestOptimizerSafetyCountStoreFastPathSupportsNodeCountStar(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH (:Group) RETURN count(*) AS total`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	require.Equal(t, "select count(*)::int8 as total from node n0 where n0.kind_ids operator (pg_catalog.@>) array [8]::int2[];", strings.Join(strings.Fields(formattedQuery), " "))
}

func TestOptimizerSafetyCountStoreFastPathUsesBaseEdgeCount(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH ()-[r:MemberOf]->() RETURN count(r)`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringProjectionPruning, "superseded by CountStoreFastPath")
	require.Equal(t, "select count(*)::int8 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [10]::int2[]);", strings.Join(strings.Fields(formattedQuery), " "))
}

// TestOptimizerSafetyCountStoreFastPathUsesSparseEdgeKindCount verifies a typed edge count reads only the selected kind's sparse count.
func TestOptimizerSafetyCountStoreFastPathUsesSparseEdgeKindCount(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH ()-[r:SuffixEdgeOne]->() RETURN count(r)`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringProjectionPruning, "superseded by CountStoreFastPath")
	require.NotContains(t, normalizedQuery, "with recursive")
	require.NotContains(t, normalizedQuery, "ordered_edges_to_path")
	require.Equal(t, "select count(*)::int8 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [4]::int2[]);", normalizedQuery)
}

func TestOptimizerSafetyCountStoreFastPathUsesUntypedEdgeCount(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH ()-[r]->() RETURN count(r)`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringProjectionPruning, "superseded by CountStoreFastPath")
	require.NotContains(t, normalizedQuery, "with recursive")
	require.NotContains(t, normalizedQuery, "ordered_edges_to_path")
	require.Equal(t, "select count(*)::int8 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id;", normalizedQuery)
}

func TestOptimizerSafetyCountStoreFastPathSupportsEdgeCountStar(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH ()-[:MemberOf]->() RETURN count(*)`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringProjectionPruning, "superseded by CountStoreFastPath")
	require.Equal(t, "select count(*)::int8 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [10]::int2[]);", strings.Join(strings.Fields(formattedQuery), " "))
}

// TestOptimizerSafetyFixedSuffixQueryPrunesExpansionEdgeCarry verifies that unobserved expansion edges are omitted from recursive state.
func TestOptimizerSafetyFixedSuffixQueryPrunesExpansionEdgeCarry(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, optimizerFixedSuffixQuery)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requirePlannedOptimizationLowering(t, translation.Optimization, "PredicatePlacement")
	requirePlannedOptimizationLowering(t, translation.Optimization, "ExpandIntoDetection")
	requireOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requireOptimizationLowering(t, translation.Optimization, "PredicatePlacement")
	requireOptimizationLowering(t, translation.Optimization, "ExpandIntoDetection")

	require.Contains(t, normalizedQuery, "select distinct (s0.n0).id as root_id from s0")
	require.Contains(t, normalizedQuery, "select distinct (s5.n0).id as root_id from s5")
	require.Contains(t, normalizedQuery, "select distinct (s9.n2).id as root_id from s9")
	require.Contains(t, normalizedQuery, "s5.ep0 as ep0")
	require.NotContains(t, normalizedQuery, "s5.e0 as e0")
	require.Contains(t, normalizedQuery, "ordered_edge_ids_to_path(0, s12.n0, s12.ep0 || array [s12.e1]::int8[] || array [s12.e2]::int8[] || array [s12.e3]::int8[]")
	require.Equal(t, 2, strings.Count(normalizedQuery, "ordered_edge_ids_to_path("), normalizedQuery)
	require.NotContains(t, normalizedQuery, "ordered_edges_to_path(")
	require.NotContains(t, normalizedQuery, "from unnest(")
	require.NotContains(t, normalizedQuery, "array [s12.e1]::edgecomposite[]")
	require.Contains(t, normalizedQuery, "from s5, s7")
	requireSQLContainsInOrder(t, normalizedQuery,
		"where s7.satisfied and exists (select 1 from edge e5 join node n6",
		"properties -> 'eligible'",
		"join edge e6 on n6.id = e6.start_id",
		"e6.end_id = (s5.n2).id",
		"and (s5.n0).id = s7.root_id",
	)
	requireSQLContainsInOrder(t, normalizedQuery,
		"where s11.satisfied and (s9.n2).id = s11.root_id and exists",
		"from edge e8 where n7.id = e8.start_id",
		"e8.end_id = (s9.n4).id",
	)
}

// assertOptimizerSafetyRelationshipStaysComposite requires a relationship consumer to retain composite rather than scalar-ID state.
func assertOptimizerSafetyRelationshipStaysComposite(t *testing.T, cypherQuery string) {
	t.Helper()

	normalizedQuery := optimizerSafetySQL(t, cypherQuery)

	require.Contains(t, normalizedQuery, "(e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0")
	require.Contains(t, normalizedQuery, "::edgecomposite")
	require.NotContains(t, normalizedQuery, "e0.id as e0")
	require.NotContains(t, normalizedQuery, "::int8[]")
}

func TestOptimizerSafetyReferencedRelationshipStaysComposite(t *testing.T) {
	t.Parallel()

	assertOptimizerSafetyRelationshipStaysComposite(t, `
MATCH p = (n:Group)-[r:MemberOf]->(m:Group)
RETURN p, r
`)
}

func TestOptimizerSafetyRelationshipExpressionReferencesStayComposite(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		query string
	}{
		{
			name: "type return",
			query: `
MATCH p = (n:Group)-[r:MemberOf]->(m:Group)
RETURN p, type(r)
`,
		},
		{
			name: "property predicate",
			query: `
MATCH p = (n:Group)-[r:MemberOf]->(m:Group)
WHERE r.label = 'member'
RETURN p
`,
		},
		{
			name: "start node return",
			query: `
MATCH p = (n:Group)-[r:MemberOf]->(m:Group)
RETURN p, startNode(r)
`,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			assertOptimizerSafetyRelationshipStaysComposite(t, testCase.query)
		})
	}
}

func TestOptimizerSafetyOptionalMatchPathStaysComposite(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH (n:Group)
OPTIONAL MATCH p = (n)-[:MemberOf]->(m:Group)
RETURN n, p
`)

	require.Contains(t, normalizedQuery, "::edgecomposite[]")
	require.NotContains(t, normalizedQuery, "::int8[]")
}

func TestOptimizerSafetyFixedHopExpandIntoUsesBoundEndpoints(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
MATCH (a:Group)
MATCH (b:Group)
MATCH p = (a)-[:MemberOf]->(b)
RETURN p
`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "(s1.n0).id = e0.start_id")
	require.Contains(t, normalizedQuery, "(s1.n1).id = e0.end_id")
	require.NotContains(t, normalizedQuery, "join node")
	require.NotNil(t, translation.Optimization.LoweringPlan)
	require.NotEmpty(t, translation.Optimization.LoweringPlan.ExpandInto)
	requirePlannedOptimizationLowering(t, translation.Optimization, "ExpandIntoDetection")
	requireOptimizationLowering(t, translation.Optimization, "ExpandIntoDetection")
}

func TestOptimizerSafetyFixedHopExpandIntoPreservesCarriedOuterMultiplicity(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
		MATCH (a:Group), (b:User)
		WITH a, b, [1, 2] AS copies
		UNWIND copies AS copy
		MATCH (a)-[:MemberOf|AdminTo]->(b)
		RETURN copy
	`)

	require.Contains(t, normalizedQuery, "from s0 join edge e0 on (s0.n0).id = e0.start_id and (s0.n1).id = e0.end_id, unnest(i0) as i1")
	require.NotContains(t, normalizedQuery, "join node")
}

func TestOptimizerSafetyFixedHopExpandIntoScopesNodeUnwindBeforePairPredicate(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
		MATCH (a:Group), (b:User)
		WITH collect(a) AS sources, b
		UNWIND sources AS source
		MATCH (source)-[:MemberOf]->(b)
		RETURN source
	`)

	require.Contains(t, normalizedQuery, "from s0, edge e0, unnest(i0) as i1 where")
	require.Contains(t, normalizedQuery, "i1.id = e0.start_id and (s0.n1).id = e0.end_id")
	require.NotContains(t, normalizedQuery, "join edge e0 on i1.id")
}

func TestOptimizerSafetyDirectionlessExpandIntoUsesPairwiseEndpoints(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
		MATCH (a:Group), (b:User)
		MATCH (a)-[:MemberOf]-(b)
		RETURN a, b
	`)

	require.Contains(t, normalizedQuery, "(((s1.n0).id = e0.start_id and (s1.n1).id = e0.end_id) or ((s1.n1).id = e0.start_id and (s1.n0).id = e0.end_id))")
	require.NotContains(t, normalizedQuery, "(s1.n0).id <> (s1.n1).id")
}

// TestOptimizerSafetyReordersIndependentNodeAnchor verifies an independent selective node can become the traversal anchor without changing semantics.
func TestOptimizerSafetyReordersIndependentNodeAnchor(t *testing.T) {
	t.Parallel()

	var (
		normalizedQuery = optimizerSafetySQL(t, `
		MATCH (a)
		MATCH (b:SuffixNodeOne {name: 'target'})
		MATCH p = (a)-[:MemberOf]->(b)
		RETURN p
		`)
		enterpriseAnchorIndex = strings.Index(normalizedQuery, "array [5]::int2[]")
		broadScanIndex        = strings.Index(normalizedQuery, "from s0, node n1")
	)

	require.NotEqual(t, -1, enterpriseAnchorIndex)
	require.NotEqual(t, -1, broadScanIndex)
	require.Less(t, enterpriseAnchorIndex, broadScanIndex)
	require.Contains(t, normalizedQuery, "(s1.n1).id = e0.start_id")
	require.Contains(t, normalizedQuery, "(s1.n0).id = e0.end_id")
}

// TestOptimizerSafetyExpansionTerminalPushdownForFixedSuffix verifies an eligible fixed suffix is pushed into terminal expansion filtering.
func TestOptimizerSafetyExpansionTerminalPushdownForFixedSuffix(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH p = (n:Group)-[:MemberOf*1..]->(m)-[:SuffixEdgeOne]->(ca:SuffixNodeOne)
RETURN p
`)

	require.Contains(t, normalizedQuery, "exists (select 1 from edge e1 join node n2")
	require.Contains(t, normalizedQuery, "n1.id = e1.start_id")
	require.Contains(t, normalizedQuery, "e1.kind_id = any (array [4]::int2[])")
	require.Contains(t, normalizedQuery, "n2.kind_ids operator (pg_catalog.@>) array [5]::int2[]")
}

// TestOptimizerSafetySuffixPredicatePlacementStaysInsideTerminalExists verifies suffix predicates remain scoped to the terminal existence check.
func TestOptimizerSafetySuffixPredicatePlacementStaysInsideTerminalExists(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH p = (n:Group)-[:MemberOf*1..]->(m)-[:SuffixEdgeOne]->(ca:SuffixNodeOne)
WHERE ca.name = 'target'
RETURN p
`)

	requireSQLContainsInOrder(t, normalizedQuery,
		"exists (select 1 from edge e1 join node n2",
		"properties -> 'name'",
		"where n1.id = e1.start_id",
	)
}

// TestOptimizerSafetyPredicatePlacementRecordsExpansionRootConstraint verifies root predicates are recorded and emitted at the expansion root.
func TestOptimizerSafetyPredicatePlacementRecordsExpansionRootConstraint(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (src:Group)-[:MemberOf*1..]->(mid)-[:SuffixEdgeOne]->(ca:SuffixNodeOne)
WHERE src.name = 'source'
RETURN p
`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringPredicatePlacement)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringPredicatePlacement)
	requireNoSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringPredicatePlacement)
	requireSQLContainsInOrder(t, normalizedQuery,
		"select n0.id as root_id from node n0 where",
		"properties -> 'name'",
	)
}

func TestOptimizerSafetyPredicatePlacementRecordsFixedTraversalConstraint(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (src:Group)-[:MemberOf]->(dst)
WHERE src.name = 'source'
RETURN dst
`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringPredicatePlacement)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringPredicatePlacement)
	requireNoSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringPredicatePlacement)
	requireSQLContainsInOrder(t, normalizedQuery,
		"join node n0 on",
		"properties -> 'name'",
		"join node n1",
	)
}

func TestOptimizerSafetyPatternPredicateExistencePlacementIsPlanned(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (s)
WHERE NOT (s)-[]-()
RETURN s
`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "not exists (select 1 from edge e0")
	requirePlannedOptimizationLowering(t, translation.Optimization, "PredicatePlacement")
	requireOptimizationLowering(t, translation.Optimization, "PredicatePlacement")
}

// TestOptimizerSafetyContinuationRelationshipsExcludePriorPathRelationships verifies suffix traversal cannot reuse relationships from the expanded prefix.
func TestOptimizerSafetyContinuationRelationshipsExcludePriorPathRelationships(t *testing.T) {
	t.Parallel()

	expandedPrefixQuery := optimizerSafetySQL(t, `
MATCH p = (n:Group)-[:MemberOf*1..]->(m)-[:SuffixEdgeOne]-(ca:SuffixNodeOne)
RETURN p
`)

	require.Contains(t, expandedPrefixQuery, "e1.id != all")
	require.Contains(t, expandedPrefixQuery, "ep0")

	fixedPrefixQuery := optimizerSafetySQL(t, `
MATCH p = (n:Group)-[:MemberOf]->(m)-[:SuffixEdgeOne]->(ca:SuffixNodeOne)
RETURN p
`)

	require.Contains(t, fixedPrefixQuery, "e1.id != s0.e0")
}

// TestOptimizerSafetyDirectionBalancedExpansionDoesNotPlanStaleSuffixPushdown verifies reoriented traversal targets do not retain obsolete suffix decisions.
func TestOptimizerSafetyDirectionBalancedExpansionDoesNotPlanStaleSuffixPushdown(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (n)-[:MemberOf*1..]->(ca:SuffixNodeOne)-[:SuffixEdgeTwo]->(d:Domain)
RETURN p
	`)

	requirePlannedOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireNoPlannedOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requireNoOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
}

func TestOptimizerSafetyTraversalDirectionUsesRightEndpointPredicate(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (n)-[:MemberOf*1..]->(ca)
WHERE ca.name = 'target'
RETURN p
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	require.Contains(t, normalizedQuery, "jsonb_typeof((n1.properties -> 'name')) = 'string'")
	require.Contains(t, normalizedQuery, "(n1.properties ->> 'name') = 'target'")
	require.Contains(t, normalizedQuery, "join edge e0 on e0.end_id = s1_seed.root_id")
}

func TestOptimizerSafetyExactOneHopRangeUsesFixedTraversal(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (a:Group)-[:MemberOf*1..1]->(b:Group)
WHERE a.name = 'src'
RETURN p
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(strings.ToLower(formattedQuery)), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringExactRangeExpansion)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringExactRangeExpansion)
	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringLatePathMaterialization)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringLatePathMaterialization)
	require.NotContains(t, normalizedQuery, "with recursive")
	require.NotContains(t, normalizedQuery, "_seed")
	require.Contains(t, normalizedQuery, "from edge e0 join node n0")
	require.Contains(t, normalizedQuery, "join node n1")
}

func TestOptimizerSafetyExactTwoHopRangeUsesFixedTraversal(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (a:Group)-[:MemberOf*2..2]->(b:Group)
WHERE a.name = 'src'
RETURN p
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(strings.ToLower(formattedQuery)), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringExactRangeExpansion)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringExactRangeExpansion)
	require.NotContains(t, normalizedQuery, "with recursive")
	require.NotContains(t, normalizedQuery, "_seed")
	require.Contains(t, normalizedQuery, "from edge e0")
	require.Contains(t, normalizedQuery, "join edge e1")
	require.Contains(t, normalizedQuery, "e1.id !=")
	require.Contains(t, normalizedQuery, "array [")
}

// TestOptimizerSafetyExactTwoHopRangePreservesLaterSourceStepTargets verifies exact-range expansion does not renumber later source-step decisions.
func TestOptimizerSafetyExactTwoHopRangePreservesLaterSourceStepTargets(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (a)-[:MemberOf*2..2]->(b)-[:SuffixEdgeOne]->(c)
RETURN a
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringExactRangeExpansion)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringExactRangeExpansion)
	require.Contains(t, normalizedQuery, "on s1.n2 = e2.start_id")
	require.NotContains(t, normalizedQuery, "on n2.id = e2.start_id")
}

// TestOptimizerSafetyExactTwoHopRangeCarriesSyntheticIntermediateNodeID verifies that exact-range lowering retains the intermediate join identity.
func TestOptimizerSafetyExactTwoHopRangeCarriesSyntheticIntermediateNodeID(t *testing.T) {
	t.Parallel()

	normalizedQuery := strings.ToLower(optimizerSafetySQL(t, `
MATCH (a)-[:MemberOf*2..2]->(b)
RETURN a
	`))

	require.Contains(t, normalizedQuery, "on s0.n1 = e1.start_id")
	require.NotContains(t, normalizedQuery, "on n1.id = e1.start_id")
}

// TestOptimizerSafetyConsecutiveExactRangesUseSourceStepTargets verifies consecutive expansions retain their original source-step coordinates.
func TestOptimizerSafetyConsecutiveExactRangesUseSourceStepTargets(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (a)-[:MemberOf*2..2]->(b)-[:SuffixEdgeOne*1..1]->(c)
RETURN p
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(strings.ToLower(formattedQuery)), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringExactRangeExpansion)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringExactRangeExpansion)
	requireNoSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringExactRangeExpansion)
	require.NotContains(t, normalizedQuery, "with recursive")
	require.NotContains(t, normalizedQuery, "_seed")
	require.Contains(t, normalizedQuery, "join edge e2")
}

// TestOptimizerSafetyExactRangePrefixPreservesSuffixPushdownTargets verifies prefix expansion leaves fixed-suffix decisions keyed to source coordinates.
func TestOptimizerSafetyExactRangePrefixPreservesSuffixPushdownTargets(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (a)-[:MemberOf*2..2]->(b)-[:AdminTo*1..]->(c)-[:SuffixEdgeOne]->(d)
RETURN p
	`)

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringExactRangeExpansion)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringExactRangeExpansion)
	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringExpansionSuffixPushdown)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringExpansionSuffixPushdown)
	requireNoSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringExpansionSuffixPushdown)
}

func TestOptimizerSafetyPathRelationshipPredicateUsesPathIDs(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (a:Group)-[:MemberOf*1..]->(b:Group)
WHERE any(r in relationships(p) WHERE type(r) STARTS WITH 'Member')
RETURN p
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(strings.ToLower(formattedQuery)), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringPathRelationshipPredicate)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringPathRelationshipPredicate)
	require.Contains(t, normalizedQuery, "exists (select 1 from edge i0 where")
	require.Contains(t, normalizedQuery, "i0.id = any (s0.ep0)")
	require.Contains(t, normalizedQuery, "kind_name(i0.kind_id)::text like 'member%'")
	require.NotContains(t, normalizedQuery, "select count(*)::int from unnest")
	require.NotContains(t, normalizedQuery, "from unnest(((select coalesce(array_agg")
}

func TestOptimizerSafetyNonePathRelationshipPredicateUsesNotExists(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (a:Group)-[:MemberOf*1..]->(b:Group)
WHERE none(r in relationships(p) WHERE type(r) = 'AdminTo')
RETURN p
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(strings.ToLower(formattedQuery)), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringPathRelationshipPredicate)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringPathRelationshipPredicate)
	require.Contains(t, normalizedQuery, "not exists (select 1 from edge i0 where")
	require.Contains(t, normalizedQuery, "i0.id = any (s0.ep0)")
	require.Contains(t, normalizedQuery, "s0.ep0 is not null")
	require.NotContains(t, normalizedQuery, "select count(*)::int from unnest")
}

func TestOptimizerSafetyAggregateTraversalCountUsesIDOnlySourceAnchoredShape(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (u:User)
WHERE u.hasspn = true AND u.enabled = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	var (
		normalizedQuery = strings.Join(strings.Fields(formattedQuery), " ")
		lowerQuery      = strings.ToLower(normalizedQuery)
	)

	requirePlannedOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireNoOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireSkippedOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection", "superseded by AggregateTraversalCount")
	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	require.Contains(t, lowerQuery, "with recursive candidate_sources(root_id)")
	require.Contains(t, lowerQuery, "traversal(root_id, next_id, depth, path)")
	require.Contains(t, lowerQuery, "terminal_nodes(id) as materialized")
	require.Contains(t, lowerQuery, "terminal_hits(root_id)")
	require.Contains(t, lowerQuery, "ranked(root_id, admincount)")
	require.Contains(t, lowerQuery, "join edge e on e.start_id = candidate_sources.root_id")
	require.Contains(t, lowerQuery, "e.start_id = traversal.next_id")
	require.Contains(t, lowerQuery, "e.id != all (traversal.path)")
	require.Contains(t, lowerQuery, "join terminal_nodes on terminal_nodes.id = traversal.next_id")
	require.Contains(t, lowerQuery, "count(*)::int8 as admincount")
	require.Contains(t, lowerQuery, "group by terminal_hits.root_id")
	require.Contains(t, lowerQuery, "from ranked join node source_node on source_node.id = ranked.root_id")
	require.NotContains(t, lowerQuery, "group by (")
	require.NotContains(t, lowerQuery, "::nodecomposite as n0 from")
}

func TestOptimizerSafetyTraversalDirectionReportsKindOnlyTerminalSkip(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (u:User)
WHERE u.hasspn = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
RETURN count(c)
	`)

	requirePlannedOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireNoOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireSkippedOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection", "terminal kind-only estimate too broad")
}

func TestOptimizerSafetyTraversalDirectionReportsSelectiveSourceSkip(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (u:User)
WHERE u.objectid = 'S-1-5-21-1-1100'
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer {name: 'target'})
RETURN c
	`)

	requirePlannedOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireNoOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireSkippedOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection", "bound source estimate selective")
}

func TestOptimizerSafetyTraversalDirectionReportsPriorLimitSourceSkip(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (u:User)
WHERE u.hasspn = true
WITH u
LIMIT 10
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer {name: 'target'})
RETURN c
	`)

	requirePlannedOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireNoOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireSkippedOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection", "bound source estimate selective")
}

func TestOptimizerSafetyAggregateTraversalCountAcceptsRowCount(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (u:User)
WHERE u.hasspn = true AND u.enabled = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, COUNT(*) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(strings.ToLower(formattedQuery)), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	require.Contains(t, normalizedQuery, "count(*)::int8 as admincount")
	require.Contains(t, normalizedQuery, "group by terminal_hits.root_id")
}

func TestOptimizerSafetyAggregateTraversalCountHonorsExplicitDepthBounds(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (u:User)
WHERE u.hasspn = true
MATCH (u)-[:MemberOf|AdminTo*2..4]->(c:Computer)
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(strings.ToLower(formattedQuery)), " ")

	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	require.Contains(t, normalizedQuery, "where traversal.depth < 4")
	require.Contains(t, normalizedQuery, "where traversal.depth >= 2")
}

func TestOptimizerSafetyAggregateTraversalCountSupportsInboundSourceAnchoring(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (u:User)
WHERE u.hasspn = true
MATCH (u)<-[:MemberOf|AdminTo*1..]-(c:Computer)
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(strings.ToLower(formattedQuery)), " ")

	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	require.Contains(t, normalizedQuery, "join edge e on e.end_id = candidate_sources.root_id")
	require.Contains(t, normalizedQuery, "e.end_id = traversal.next_id")
}

func TestOptimizerSafetyAggregateTraversalCountReturnsCountAlias(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (u:User)
WHERE u.hasspn = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u AS user, adminCount AS privileges
ORDER BY privileges DESC
LIMIT 100
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(strings.ToLower(formattedQuery)), " ")

	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	require.Contains(t, normalizedQuery, "(source_node.id, source_node.kind_ids, source_node.properties)::nodecomposite as user")
	require.Contains(t, normalizedQuery, "ranked.admincount as privileges")
	require.Contains(t, normalizedQuery, "order by ranked.admincount desc")
}

func TestOptimizerSafetyAggregateTraversalCountFoldsTerminalFilter(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (u:User)
WHERE u.hasspn = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WHERE c.enabled = true
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(strings.ToLower(formattedQuery)), " ")

	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	require.Contains(t, normalizedQuery, "terminal_nodes(id) as materialized")
	require.Contains(t, normalizedQuery, "terminal_node.properties -> 'enabled'")
	require.Contains(t, normalizedQuery, "join terminal_nodes on terminal_nodes.id = traversal.next_id")
}

func TestOptimizerSafetyAggregateTraversalCountUsesDistinctPredicateParameters(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslationWithParameters(t, `
MATCH (u:User)
WHERE u.enabled = $sourceEnabled
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WHERE c.enabled = $terminalEnabled
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100
	`, map[string]any{
		"sourceEnabled":   true,
		"terminalEnabled": false,
	})
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(strings.ToLower(formattedQuery)), " ")

	parameterValues := make([]any, 0, len(translation.Parameters))
	for _, value := range translation.Parameters {
		parameterValues = append(parameterValues, value)
	}

	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	require.Contains(t, normalizedQuery, "source_node.properties -> 'enabled'")
	require.Contains(t, normalizedQuery, "terminal_node.properties -> 'enabled'")
	require.Len(t, translation.Parameters, 2)
	require.ElementsMatch(t, []any{true, false}, parameterValues)
}

func TestOptimizerSafetyAggregateTraversalCountReusesPredicateParameter(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslationWithParameters(t, `
MATCH (u:User)
WHERE u.enabled = $enabled
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WHERE c.enabled = $enabled
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100
	`, map[string]any{
		"enabled": true,
	})
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(strings.ToLower(formattedQuery)), " ")

	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	require.Contains(t, normalizedQuery, "source_node.properties -> 'enabled'")
	require.Contains(t, normalizedQuery, "terminal_node.properties -> 'enabled'")
	require.Len(t, translation.Parameters, 1)
}

func TestOptimizerSafetyAggregateTraversalCountSkipsUnsafeWideningCandidates(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		query string
	}{{
		name: "distinct terminal count",
		query: `
MATCH (u:User)
WHERE u.hasspn = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, COUNT(DISTINCT c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100
		`,
	}, {
		name: "optional traversal",
		query: `
MATCH (u:User)
WHERE u.hasspn = true
OPTIONAL MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100
		`,
	}, {
		name: "path binding observed",
		query: `
MATCH (u:User)
WHERE u.hasspn = true
MATCH p = (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, p, COUNT(c) AS adminCount
RETURN u, p
ORDER BY adminCount DESC
LIMIT 100
		`,
	}, {
		name: "relationship binding observed",
		query: `
MATCH (u:User)
WHERE u.hasspn = true
MATCH (u)-[r:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, r, COUNT(c) AS adminCount
RETURN u, r
ORDER BY adminCount DESC
LIMIT 100
		`,
	}, {
		name: "correlated terminal filter",
		query: `
MATCH (u:User)
WHERE u.hasspn = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WHERE c.name = u.name
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100
		`,
	}, {
		name: "post aggregation filter",
		query: `
MATCH (u:User)
WHERE u.hasspn = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, COUNT(c) AS adminCount
WHERE adminCount > 1
RETURN u
ORDER BY adminCount DESC
LIMIT 100
		`,
	}}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			translation := optimizerSafetyTranslation(t, testCase.query)

			requireNoPlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
			requireNoOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
		})
	}
}

func TestOptimizerSafetyAggregateTraversalCountSkipsParameterizedCorrelatedTerminalFilter(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslationWithParameters(t, `
MATCH (u:User)
WHERE u.enabled = $enabled
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WHERE c.name = u.name AND c.enabled = $enabled
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100
	`, map[string]any{
		"enabled": true,
	})

	requireNoPlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	requireNoOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
}

func TestOptimizerSafetyAggregateTraversalCountSkipsObservedTerminal(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (u:User)
WHERE u.hasspn = true AND u.enabled = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, c, COUNT(c) AS adminCount
RETURN u, c
ORDER BY adminCount DESC
LIMIT 100
	`)

	requireNoPlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	requireNoOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
}

func TestOptimizerSafetyShortestPathStrategyUsesPlannedBidirectionalSearch(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = allShortestPaths((s)-[:MemberOf*1..]->(e))
WHERE s.name = 'source' AND e.name = 'target'
RETURN p
	`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "bidirectional_asp_harness")
	requirePlannedOptimizationLowering(t, translation.Optimization, "ShortestPathStrategySelection")
	requirePlannedOptimizationLowering(t, translation.Optimization, "ShortestPathFilterMaterialization")
	requireOptimizationLowering(t, translation.Optimization, "ShortestPathStrategySelection")
	requireOptimizationLowering(t, translation.Optimization, "ShortestPathFilterMaterialization")
}

func TestOptimizerSafetyShortestPathTerminalFilterUsesPlannedMaterialization(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (s:Group {name: 'source'})
MATCH p = shortestPath((s)-[:MemberOf*1..]->(e))
WHERE e.name = 'target'
RETURN p
	`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "unidirectional_sp_harness")
	require.Contains(t, normalizedQuery, "traversal_terminal_filter")
	requirePlannedOptimizationLowering(t, translation.Optimization, "ShortestPathFilterMaterialization")
	requireOptimizationLowering(t, translation.Optimization, "ShortestPathFilterMaterialization")
}

func TestOptimizerSafetyShortestPathKindOnlyTerminalFilterUsesPlannedMaterialization(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = shortestPath((s:Group)-[:MemberOf|GenericAll|AdminTo*1..]->(t:Tag_Tier_Zero))
WHERE s.objectid ENDS WITH '-513' AND s <> t
RETURN p
LIMIT 1000
	`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "unidirectional_sp_harness")
	require.Contains(t, normalizedQuery, "traversal_terminal_filter")
	requirePlannedOptimizationLowering(t, translation.Optimization, "ShortestPathFilterMaterialization")
	requireOptimizationLowering(t, translation.Optimization, "ShortestPathFilterMaterialization")
}

func TestOptimizerSafetyLimitPushdownUsesPlannedTraversalFrame(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (n:Group)-[:MemberOf]->(m:Group)
RETURN p
LIMIT 1
	`)

	requirePlannedOptimizationLowering(t, translation.Optimization, "LimitPushdown")
	requireOptimizationLowering(t, translation.Optimization, "LimitPushdown")
}

func TestOptimizerSafetyShortestPathLimitPushdownUsesPlannedHarness(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = shortestPath((s)-[:MemberOf*1..]->(e))
WHERE s.name = 'source' AND e.name = 'target'
RETURN p
LIMIT 1
	`)

	requirePlannedOptimizationLowering(t, translation.Optimization, "LimitPushdown")
	requireOptimizationLowering(t, translation.Optimization, "LimitPushdown")
}

func TestOptimizerSafetyShortestPathRootCarriesUnwindSources(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
		UNWIND ['source'] AS sourceName
		MATCH p = shortestPath((s:Group)-[:MemberOf*1..]->(e:Group))
		WHERE s.name = sourceName AND e.name = 'target'
		RETURN sourceName, p
	`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "unidirectional_sp_harness")
	require.Contains(t, normalizedQuery, "unnest(array ['source']::text[]) as i0")
	requirePlanParameterContains(t, translation, "jsonb_typeof((n1.properties -> 'name')) = 'string'")
	requirePlanParameterContains(t, translation, "(n0.properties ->> 'name') = i0")
}

func TestOptimizerSafetyShortestPathTerminalCarriesUnwindSources(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
		UNWIND ['target'] AS targetName
		MATCH p = shortestPath((s:Group)-[:MemberOf*1..]->(e:Group))
		WHERE s.name = 'source' AND e.name = targetName
		RETURN targetName, p
	`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "unidirectional_sp_harness")
	require.Contains(t, normalizedQuery, "unnest(array ['target']::text[]) as i0")
	requirePlanParameterContains(t, translation, "(n1.properties ->> 'name') = i0")
}

// TestOptimizerSafetyTranslationReportsOptimizerMetadata verifies translation reports planned, applied, and targeted lowering diagnostics.
func TestOptimizerSafetyTranslationReportsOptimizerMetadata(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
MATCH p = (n:Group)-[:MemberOf*1..]->(m)-[:SuffixEdgeOne]->(ca:SuffixNodeOne)
WHERE ca.name = 'target'
RETURN p
`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)

	require.NotEmpty(t, translation.Optimization.Rules)
	require.NotEmpty(t, translation.Optimization.PredicateAttachments)
	require.NotNil(t, translation.Optimization.LoweringPlan)
	require.NotEmpty(t, translation.Optimization.LoweringPlan.ProjectionPruning)
	require.NotEmpty(t, translation.Optimization.LoweringPlan.LatePathMaterialization)
	require.NotEmpty(t, translation.Optimization.LoweringPlan.ExpansionSuffixPushdown)
	require.NotEmpty(t, translation.Optimization.LoweringPlan.PredicatePlacement)
	requirePlannedOptimizationLowering(t, translation.Optimization, "ProjectionPruning")
	requirePlannedOptimizationLowering(t, translation.Optimization, "LatePathMaterialization")
	requirePlannedOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requirePlannedOptimizationLowering(t, translation.Optimization, "PredicatePlacement")
	requireOptimizationLowering(t, translation.Optimization, "ProjectionPruning")
	requireOptimizationLowering(t, translation.Optimization, "LatePathMaterialization")
	requireOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requireOptimizationLowering(t, translation.Optimization, "PredicatePlacement")
}

// TestOptimizerSafetyExpansionTerminalPushdownForZeroDepthExpansion verifies terminal filtering preserves the zero-edge expansion alternative.
func TestOptimizerSafetyExpansionTerminalPushdownForZeroDepthExpansion(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH p = (n:Group)-[:MemberOf*0..]->(m)-[:SuffixEdgeOne]->(ca:SuffixNodeOne)
RETURN p
`)

	require.Contains(t, normalizedQuery, "exists (select 1 from edge e1 join node n2")
	require.Contains(t, normalizedQuery, "n1.id = e1.start_id")
	require.Contains(t, normalizedQuery, "e1.kind_id = any (array [4]::int2[])")
	require.Contains(t, normalizedQuery, "n2.kind_ids operator (pg_catalog.@>) array [5]::int2[]")
}

// TestOptimizerSafetyExpansionTerminalPushdownForBoundEndpointSuffixChain verifies a bound suffix endpoint is honored inside supplemental search.
func TestOptimizerSafetyExpansionTerminalPushdownForBoundEndpointSuffixChain(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH (ca:SuffixNodeOne {name: 'target'})
MATCH p = (n:Group)-[:MemberOf*0..]->(m)-[:SuffixEdgeOne]->(ct:CertTemplate)-[:PublishedTo]->(ca)
WHERE ct.authenticationenabled = true
RETURN p
`)

	require.Contains(t, normalizedQuery, "exists (select 1 from edge e1 join node n3")
	require.Contains(t, normalizedQuery, "join edge e2 on n3.id = e2.start_id")
	require.Contains(t, normalizedQuery, "n2.id = e1.start_id")
	require.Contains(t, normalizedQuery, "e1.kind_id = any")
	require.Contains(t, normalizedQuery, "n3.kind_ids operator (pg_catalog.@>)")
	require.Contains(t, normalizedQuery, "e2.kind_id = any")
	require.Contains(t, normalizedQuery, "e2.end_id = (s0.n0).id")
	requireSQLContainsInOrder(t, normalizedQuery,
		"exists (select 1 from edge e1 join node n3",
		"properties -> 'authenticationenabled'",
		"join edge e2 on n3.id = e2.start_id",
		"e2.end_id = (s0.n0).id",
	)
}

// TestOptimizerSafetyExpansionTerminalPushdownIncludesConstrainedBoundEndpoint verifies bound-endpoint predicates are included in terminal filtering.
func TestOptimizerSafetyExpansionTerminalPushdownIncludesConstrainedBoundEndpoint(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (ca)
MATCH p = (n:Group)-[:MemberOf*0..]->(m)-[:SuffixEdgeOne]->(ct:CertTemplate)-[:PublishedTo]->(ca:SuffixNodeOne)
RETURN p
`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requireOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requireSQLContainsInOrder(t, normalizedQuery,
		"exists (select 1 from edge e1 join node n3",
		"join edge e2 on n3.id = e2.start_id",
		"e2.end_id = (s0.n0).id",
	)
	require.Contains(t, normalizedQuery, "(s0.n0).kind_ids operator (pg_catalog.@>)")
}

// TestOptimizerSafetyExpansionTerminalPushdownForBoundDomainSuffix verifies domain-bound suffix nodes remain constrained during supplemental search.
func TestOptimizerSafetyExpansionTerminalPushdownForBoundDomainSuffix(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH (d:Domain {name: 'target'})
MATCH p = (ca:SuffixNodeOne)-[:IssuedSignedBy|SuffixNodeOneFor*1..]->(root:RootCA)-[:RootCAFor]->(d)
RETURN p
`)

	require.Contains(t, normalizedQuery, "exists (select 1 from edge e1")
	require.Contains(t, normalizedQuery, "e1.kind_id = any")
	require.Contains(t, normalizedQuery, "n2.kind_ids operator (pg_catalog.@>)")
	require.Contains(t, normalizedQuery, "n2.id = e1.start_id")
	require.Contains(t, normalizedQuery, "e1.end_id = (s0.n0).id")
}

// TestOptimizerSafetyExpansionTerminalPushdownForInboundFixedSuffix verifies inbound suffix direction is preserved in terminal filtering.
func TestOptimizerSafetyExpansionTerminalPushdownForInboundFixedSuffix(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH p = (ca:SuffixNodeOne)<-[:PublishedTo*1..]-(ct)<-[:SuffixEdgeOne]-(m:Group)
RETURN p
`)

	require.Contains(t, normalizedQuery, "exists (select 1 from edge e1 join node n2")
	require.Contains(t, normalizedQuery, "n1.id = e1.end_id")
	require.Contains(t, normalizedQuery, "e1.kind_id = any (array [4]::int2[])")
	require.Contains(t, normalizedQuery, "n2.kind_ids operator (pg_catalog.@>)")
}

// TestOptimizerSafetyExpansionTerminalPushdownSkipsDirectionlessSuffix verifies undirected suffixes are excluded from terminal-filter pushdown.
func TestOptimizerSafetyExpansionTerminalPushdownSkipsDirectionlessSuffix(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH p = (n:Group)-[:MemberOf*1..]->(m)-[:SuffixEdgeOne]-(ca:SuffixNodeOne)
RETURN p
`)

	require.NotContains(t, normalizedQuery, "exists (select 1 from edge e1 join node n2")
}
