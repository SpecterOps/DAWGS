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

func optimizerSafetySQL(t *testing.T, cypherQuery string) string {
	t.Helper()

	translation := optimizerSafetyTranslation(t, cypherQuery)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	return strings.Join(strings.Fields(formattedQuery), " ")
}

func optimizerSafetyTranslation(t *testing.T, cypherQuery string) Result {
	t.Helper()

	return optimizerSafetyTranslationWithParameters(t, cypherQuery, nil)
}

func optimizerSafetyTranslationWithParameters(t *testing.T, cypherQuery string, parameters map[string]any) Result {
	t.Helper()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), parameters, DefaultGraphID)
	require.NoError(t, err)

	return translation
}

func requireOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.Lowerings {
		if lowering.Name == name {
			return
		}
	}

	require.Failf(t, "missing optimization lowering", "expected lowering %q in %#v", name, summary.Lowerings)
}

func requireNoOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.Lowerings {
		require.NotEqualf(t, name, lowering.Name, "unexpected applied lowering %q in %#v", name, summary.Lowerings)
	}
}

func requirePlannedOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.PlannedLowerings {
		if lowering.Name == name {
			return
		}
	}

	require.Failf(t, "missing planned optimization lowering", "expected planned lowering %q in %#v", name, summary.PlannedLowerings)
}

func requireNoPlannedOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.PlannedLowerings {
		require.NotEqualf(t, name, lowering.Name, "unexpected planned lowering %q in %#v", name, summary.PlannedLowerings)
	}
}

func requirePlanParameterContains(t *testing.T, translation Result, expected string) {
	t.Helper()

	for _, parameter := range translation.Parameters {
		if planQuery, ok := parameter.(string); ok && strings.Contains(planQuery, expected) {
			return
		}
	}

	require.Failf(t, "missing plan parameter content", "expected a plan parameter to contain %q in %#v", expected, translation.Parameters)
}

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
	require.Equal(t, []string{"EXPANSION-STEPWISE-FORWARD", "EXPANSION-LATE-HYDRATED-FORWARD", "EXPANSION-FACTORED-SUFFIX-FORWARD", "EXPANSION-SUFFIX-SEEDED-REVERSE", "EXPANSION-BACKWARD-VIABILITY-FORWARD"}, outcome.PlannedCandidates)
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
	require.Equal(t, "forced_tool", outcome.SelectionMode)
	require.Empty(t, outcome.SkipReason)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy)
	requireNoSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy)
}

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
	require.Equal(t, []string{"SP-S0", "SP-S0-DIRECT", "SP-S1", "SP-S2", "SP-S3-U-D", "SP-S3-U-E+MAT-M0", "SP-S4-C-D", "SP-S4-C-WE+MAT-M0"}, outcome.PlannedCandidates)
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

func TestShortestExecutorV4SelectsCompactMultiKindPathAndKeepsS3Distance(t *testing.T) {
	for _, test := range []struct {
		observation string
		selected    optimize.ShortestPathExecutor
		reason      string
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
	}
}

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
	require.Equal(t, []string{"SP-S0", "ASP-A1-DAG"}, outcome.PlannedCandidates)
	require.Equal(t, string(optimize.ShortestPathObservationAllPaths), outcome.ObservationMode)
	require.Equal(t, string(optimize.ShortestPathExecutorASPA1DAG), outcome.Selected)
	require.Equal(t, string(optimize.ShortestPathExecutorASPA1DAG), outcome.Applied)
	require.Equal(t, "asp-static-v1", outcome.SelectorVersion)
	require.Empty(t, outcome.SkipReason)
}

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
	require.Equal(t, "sp-static-v3", productionOutcome.SelectorVersion)

	forced, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{ForceShortestPathExecutor: optimize.ShortestPathExecutorS3EdgeM0})
	require.NoError(t, err)
	forcedSQL, err := Translated(forced)
	require.NoError(t, err)

	require.Equal(t, incumbentSQL, forcedSQL)
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

func TestForcedShortestPathEdgeM0ExecutorRejectsDistanceObservation(t *testing.T) {
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
}

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

func requireSQLContainsInOrder(t *testing.T, sql string, parts ...string) {
	t.Helper()

	offset := 0
	for _, part := range parts {
		nextIndex := strings.Index(sql[offset:], part)
		require.NotEqualf(t, -1, nextIndex, "expected SQL to contain %q after offset %d:\n%s", part, offset, sql)
		offset += nextIndex + len(part)
	}
}

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

func TestOptimizerSafetyExactTwoHopRangeCarriesSyntheticIntermediateNodeID(t *testing.T) {
	t.Parallel()

	normalizedQuery := strings.ToLower(optimizerSafetySQL(t, `
MATCH (a)-[:MemberOf*2..2]->(b)
RETURN a
	`))

	require.Contains(t, normalizedQuery, "on s0.n1 = e1.start_id")
	require.NotContains(t, normalizedQuery, "on n1.id = e1.start_id")
}

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

func TestOptimizerSafetyExpansionTerminalPushdownSkipsDirectionlessSuffix(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH p = (n:Group)-[:MemberOf*1..]->(m)-[:SuffixEdgeOne]-(ca:SuffixNodeOne)
RETURN p
`)

	require.NotContains(t, normalizedQuery, "exists (select 1 from edge e1 join node n2")
}
