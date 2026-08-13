package translate

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

// guardedSuffixOrientationQuery reserves the stable protocol value used to recognize guarded suffix orientation query across artifacts and executions.
const guardedSuffixOrientationQuery = `
	MATCH (root:ExpansionRoot)
	WHERE root.root_key = $root_key
	MATCH path = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
	RETURN path
`

// TestExpansionOrientationReverseDominanceHasStrictHysteresis verifies expansion orientation reverse dominance has strict hysteresis behavior.
func TestExpansionOrientationReverseDominanceHasStrictHysteresis(t *testing.T) {
	require.False(t, expansionOrientationReverseDominates(0, 0))
	require.False(t, expansionOrientationReverseDominates(100, 75))
	require.False(t, expansionOrientationReverseDominates(4, 3))
	require.True(t, expansionOrientationReverseDominates(100, 74))
	require.True(t, expansionOrientationReverseDominates(4, 2))
}

// TestExpansionOrientationBooleanModesRemainV1ByDefault verifies expansion orientation boolean modes remain v1 by default behavior.
func TestExpansionOrientationBooleanModesRemainV1ByDefault(t *testing.T) {
	for _, testCase := range []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// options retains the options while anonymous record is assembled or evaluated.
		options ToolOptions
	}{
		{
			name:    "guarded",
			options: ToolOptions{EnableExpansionOrientationTournament: true},
		},
		{
			name:    "shadow",
			options: ToolOptions{EnableExpansionOrientationShadow: true},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			translate := func(options ToolOptions) (Result, string) {
				regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
				require.NoError(t, err)
				translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
					"root_key": "v1-default-root",
				}, DefaultGraphID, options)
				require.NoError(t, err)
				formatted, err := Translated(translation)
				require.NoError(t, err)
				return translation, formatted
			}

			implicit, implicitSQL := translate(testCase.options)
			explicitOptions := testCase.options
			explicitOptions.ExpansionOrientationPolicy = optimize.ExpansionSearchPolicyOrientationProbeV1
			explicit, explicitSQL := translate(explicitOptions)

			require.Equal(t, implicitSQL, explicitSQL)
			require.Contains(t, implicitSQL, "(s5_orientation_metrics.suffix_rows + s5_orientation_metrics.boundary_rows + s5_orientation_metrics.reverse_degree_rows) * 4 < (s5_orientation_metrics.root_rows + s5_orientation_metrics.forward_degree_rows) * 3")
			require.NotContains(t, implicitSQL, "16 * s5_orientation_metrics.forward_degree_rows")
			require.Equal(t, implicit.Optimization.LoweringPlan.ExpansionSearchStrategy, explicit.Optimization.LoweringPlan.ExpansionSearchStrategy)
			decision := implicit.Optimization.LoweringPlan.ExpansionSearchStrategy[0]
			require.Equal(t, optimize.ExpansionSearchPolicyOrientationProbeV1, decision.PlannedPolicy)
			require.Equal(t, optimize.ExpansionSearchPolicyOrientationProbeV1, decision.EmittedPolicy)
			require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV1), decision.SelectorVersion)
			outcome := requireTraversalTargetOutcome(t, implicit.Optimization, optimize.LoweringExpansionSearchStrategy, decision.Target)
			require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV1), outcome.PlannedPolicy)
			require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV1), outcome.EmittedPolicy)
			require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV1), outcome.SelectorVersion)
		})
	}
}

// TestExpansionOrientationProbeV2IsExplicitAndDepthWeighted verifies expansion orientation probe v2 is explicit and depth weighted behavior.
func TestExpansionOrientationProbeV2IsExplicitAndDepthWeighted(t *testing.T) {
	for _, testCase := range []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// options retains the options while anonymous record is assembled or evaluated.
		options ToolOptions
		// expectedMode identifies the expected mode.
		expectedMode string
		// expectedBoundary retains the expected boundary while anonymous record is assembled or evaluated.
		expectedBoundary string
		// expectedCandidates retains the expected candidates while anonymous record is assembled or evaluated.
		expectedCandidates []optimize.ExpansionSearchStrategy
	}{
		{
			name:               "guarded",
			options:            ToolOptions{EnableExpansionOrientationTournament: true},
			expectedMode:       "guarded_tool",
			expectedBoundary:   optimize.ExpansionSearchExecutionBoundaryGuardedDualArm,
			expectedCandidates: []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchStepwiseForward, optimize.ExpansionSearchSuffixSeededReverse},
		},
		{
			name:               "shadow",
			options:            ToolOptions{EnableExpansionOrientationShadow: true},
			expectedMode:       "shadow_tool",
			expectedBoundary:   optimize.ExpansionSearchExecutionBoundaryInlineStatement,
			expectedCandidates: []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchStepwiseForward},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
			require.NoError(t, err)
			options := testCase.options
			options.ExpansionOrientationPolicy = optimize.ExpansionSearchPolicyOrientationProbeV2
			translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
				"root_key": "v2-depth-root",
			}, DefaultGraphID, options)
			require.NoError(t, err)
			formatted, err := Translated(translation)
			require.NoError(t, err)

			require.Contains(t, formatted, "(s5_orientation_metrics.suffix_rows + s5_orientation_metrics.boundary_rows + s5_orientation_metrics.reverse_degree_rows) * 4 < (s5_orientation_metrics.root_rows + 16 * s5_orientation_metrics.forward_degree_rows) * 3")
			require.Contains(t, formatted, "s5_orientation_metrics.probes_complete and (s5_orientation_metrics.suffix_rows + s5_orientation_metrics.boundary_rows + s5_orientation_metrics.reverse_degree_rows) * 4 <")
			require.NotContains(t, formatted, "(s5_orientation_metrics.root_rows + s5_orientation_metrics.forward_degree_rows) * 3")

			decision := translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0]
			require.Equal(t, int64(16), decision.MaximumDepth)
			require.Equal(t, optimize.ExpansionSearchPolicyOrientationProbeV2, decision.PlannedPolicy)
			require.Equal(t, optimize.ExpansionSearchPolicyOrientationProbeV2, decision.EmittedPolicy)
			require.Equal(t, testCase.expectedCandidates, decision.EmittedCandidates)
			require.Equal(t, testCase.expectedMode, decision.SelectionMode)
			require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV2), decision.SelectorVersion)
			require.Equal(t, testCase.expectedBoundary, decision.ExecutionBoundary)

			outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy, decision.Target)
			require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV2), outcome.PlannedPolicy)
			require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV2), outcome.EmittedPolicy)
			require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV2), outcome.SelectorVersion)
			require.Equal(t, testCase.expectedBoundary, outcome.ExecutionBoundary)
		})
	}
}

// TestBoundedAdmissionGatesAreStrictComplements verifies bounded admission gates are strict complements behavior.
func TestBoundedAdmissionGatesAreStrictComplements(t *testing.T) {
	admitted, fallback := boundedAdmissionGates(
		boundedProbeLimit{
			source: "endpoint_probe",
			limit:  32,
		},
		boundedProbeLimit{
			source: "state_probe",
			limit:  4096,
		},
	)
	require.NotNil(t, admitted)
	require.NotNil(t, fallback)

	query := pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{
		&pgsql.AliasedExpression{
			Expression: admitted,
			Alias:      models.OptionalValue[pgsql.Identifier]("admitted"),
		},
		&pgsql.AliasedExpression{
			Expression: fallback,
			Alias:      models.OptionalValue[pgsql.Identifier]("fallback"),
		},
	}}}
	rendered, err := format.Statement(query, format.NewOutputBuilder())
	require.NoError(t, err)
	require.Contains(t, rendered, "not exists")
	require.Contains(t, rendered, "offset 32 limit 1")
	require.Contains(t, rendered, "offset 4096 limit 1")
	require.Contains(t, rendered, "or exists")
}

// TestGuardedSuffixOrientationTournamentEmitsBoundedDisjointBranches verifies guarded suffix orientation tournament emits bounded disjoint branches behavior.
func TestGuardedSuffixOrientationTournamentEmitsBoundedDisjointBranches(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)

	translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "guarded-fixed-suffix-root",
	}, DefaultGraphID, ToolOptions{EnableExpansionOrientationTournament: true})
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "s5_orientation_root_probe as materialized")
	require.Contains(t, formatted, "s5_orientation_suffix_probe as materialized")
	require.Contains(t, formatted, "s5_orientation_boundaries as materialized")
	require.Contains(t, formatted, "s5_orientation_forward_degree_probe as materialized")
	require.Contains(t, formatted, "s5_orientation_reverse_degree_probe as materialized")
	require.Contains(t, formatted, "select true as sampled from s5_orientation_root_probe")
	require.Contains(t, formatted, "select true as sampled from s5_orientation_boundaries")
	require.Contains(t, formatted, "s5_orientation_metrics as materialized")
	require.Contains(t, formatted, "s5_orientation_decision as materialized")
	require.Contains(t, formatted, "s5_orientation_states as materialized")
	require.Contains(t, formatted, "s5_orientation_admission as materialized")
	require.Contains(t, formatted, "s5_orientation_executed_candidate as materialized")
	require.Contains(t, formatted, "s5_orientation_executed_incumbent as materialized")
	require.Contains(t, formatted, "record_traversal_runtime_attestation_v1('EXPANSION-SUFFIX-SEEDED-REVERSE', 'suffix_seeded_reverse', false)")
	require.Contains(t, formatted, "record_traversal_runtime_attestation_v1('EXPANSION-STEPWISE-FORWARD', 'exact_forward_incumbent'")
	require.Contains(t, formatted, "s5_orientation_incumbent as materialized")
	require.Contains(t, formatted, "limit 513")
	require.Contains(t, formatted, "limit 16385")
	require.Contains(t, formatted, "limit 4097")
	require.Contains(t, formatted, "select (s0.n0).id as root_id from s0 limit 513")
	require.NotContains(t, formatted, "select distinct (s0.n0).id as root_id from s0 limit 513")
	require.Contains(t, formatted, "select distinct s5_orientation_suffix_probe.boundary_id as boundary_id")
	require.Contains(t, formatted, "e3.id != e2.id limit 513")
	require.Contains(t, formatted, "offset 512 limit 1")
	require.Contains(t, formatted, "offset 16384 limit 1")
	require.Contains(t, formatted, "offset 4096 limit 1")
	require.Contains(t, formatted, "(s5_orientation_metrics.suffix_rows + s5_orientation_metrics.boundary_rows + s5_orientation_metrics.reverse_degree_rows) * 4 < (s5_orientation_metrics.root_rows + s5_orientation_metrics.forward_degree_rows) * 3")
	require.Contains(t, formatted, "s5_orientation_admission.use_reverse and not s5_orientation_admission.state_overflow")
	require.Contains(t, formatted, "not s5_orientation_admission.use_reverse or s5_orientation_admission.state_overflow")
	require.Contains(t, formatted, "not s5_orientation_admission.probes_complete or s5_orientation_admission.state_overflow")
	require.Equal(t, 1, strings.Count(formatted, "offset 4096 limit 1"))
	require.Contains(t, formatted, "from s5_orientation_executed_candidate join lateral")
	require.Contains(t, formatted, "s5_orientation_executed_candidate.executed offset 0")
	require.Contains(t, formatted, "s5_orientation_incumbent as materialized (with")
	require.Contains(t, formatted, "from s5_orientation_executed_incumbent join lateral")
	require.Contains(t, formatted, "s5_orientation_executed_incumbent.executed offset 0")
	require.Contains(t, formatted, "s5_orientation_reverse_gate as materialized")
	require.Contains(t, formatted, "from s5_orientation_reverse_gate join lateral")
	require.Contains(t, formatted, "s5_orientation_reverse_gate.executed offset 0")
	require.Contains(t, formatted, "s5_orientation_states as materialized (select")
	require.Contains(t, formatted, "from s5_orientation_reverse_gate join lateral (select s5_orientation_reverse.boundary_id")
	guardedSuffixProjection := regexp.MustCompile(`(?s)s5_orientation_suffix_probe as materialized \(select (.*?) from s5_orientation_root_presence`).FindStringSubmatch(formatted)
	require.Len(t, guardedSuffixProjection, 2)
	require.Contains(t, guardedSuffixProjection[1], "n1.id as boundary_id")
	require.Contains(t, guardedSuffixProjection[1], "e1.id as e1")
	require.Contains(t, guardedSuffixProjection[1], "e2.id as e2")
	require.Contains(t, guardedSuffixProjection[1], "e3.id as e3")
	require.Contains(t, guardedSuffixProjection[1], "::nodecomposite")
	require.Contains(t, formatted, "e3.id != e1.id")
	require.Contains(t, formatted, "e3.id != e2.id")
	require.Contains(t, formatted, "union all")
	require.NotContains(t, formatted, "_orientation_shadow_")
	require.NotContains(t, formatted, "s5_orientation_incumbent as materialized (with s1 as (with recursive s2_seed(root_id) as not materialized (select distinct (s0.n0).id as root_id from s0 limit")

	require.Len(t, translation.Optimization.LoweringPlan.ExpansionSearchStrategy, 1)
	decision := translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0]
	require.Equal(t, optimize.ExpansionSearchStepwiseForward, decision.SelectedStrategy)
	require.Equal(t, optimize.ExpansionSearchPolicyOrientationProbeV1, decision.EmittedPolicy)
	require.Equal(t, []optimize.ExpansionSearchStrategy{
		optimize.ExpansionSearchStepwiseForward,
		optimize.ExpansionSearchSuffixSeededReverse,
	}, decision.EmittedCandidates)
	require.Equal(t, "guarded_tool", decision.SelectionMode)
	require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV1), decision.SelectorVersion)
	require.Empty(t, decision.FallbackReason)
	require.Equal(t, optimize.ExpansionSearchProbeCaps{
		RootRowLimit:              optimize.ExpansionSearchOrientationRootRowLimit,
		ReverseSeedRowLimit:       optimize.ExpansionSearchOrientationReverseSeedRowLimit,
		DirectionalDegreeRowLimit: optimize.ExpansionSearchOrientationDirectionalDegreeRowLimit,
	}, decision.ProbeCaps)
	require.Equal(t, optimize.ExpansionSearchAdmission{
		StateLimit:             optimize.ExpansionSearchOrientationStateLimit,
		RequiresCompleteProbes: true,
		FallbackStrategy:       optimize.ExpansionSearchStepwiseForward,
	}, decision.Admission)

	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy, decision.Target)
	require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV1), outcome.EmittedPolicy)
	require.Equal(t, "guarded_dual_arm", outcome.ExecutionBoundary)
	require.Empty(t, outcome.Applied)
	require.Empty(t, outcome.SkipReason)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy)
	requireNoSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy)
}

// TestProductionCanaryExpansionOrientationUsesVersionedGuardedPolicy verifies production canary expansion orientation uses versioned guarded policy behavior.
func TestProductionCanaryExpansionOrientationUsesVersionedGuardedPolicy(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)
	translation, err := TranslateWithProductionOptions(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "guarded-fixed-suffix-root",
	}, DefaultGraphID, ProductionOptions{
		EnableExpansionOrientation: true,
		SelectorVersion:            "traversal-production-g11",
	})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "record_traversal_runtime_attestation_v1")
	require.Contains(t, formatted, "(s5_orientation_metrics.suffix_rows + s5_orientation_metrics.boundary_rows + s5_orientation_metrics.reverse_degree_rows) * 4 < (s5_orientation_metrics.root_rows + s5_orientation_metrics.forward_degree_rows) * 3")
	require.NotContains(t, formatted, "16 * s5_orientation_metrics.forward_degree_rows")
	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy,
		optimize.TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		})
	require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV1), outcome.PlannedPolicy)
	require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV1), outcome.EmittedPolicy)
	require.Equal(t, "production_canary", outcome.SelectionMode)
	require.Equal(t, "traversal-production-g11", outcome.SelectorVersion)
	require.Equal(t, "guarded_dual_arm", outcome.ExecutionBoundary)
}

// TestSuffixOrientationShadowEmitsWouldSelectMetadataAndOnlyIncumbent verifies suffix orientation shadow emits would select metadata and only incumbent behavior.
func TestSuffixOrientationShadowEmitsWouldSelectMetadataAndOnlyIncumbent(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)

	translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "shadow-fixed-suffix-root",
	}, DefaultGraphID, ToolOptions{EnableExpansionOrientationShadow: true})
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "s5_orientation_root_probe as materialized")
	require.Contains(t, formatted, "s5_orientation_suffix_probe as materialized")
	require.Contains(t, formatted, "s5_orientation_forward_degree_probe as materialized")
	require.Contains(t, formatted, "s5_orientation_reverse_degree_probe as materialized")
	require.Contains(t, formatted, "select true as sampled from s5_orientation_root_probe")
	require.Contains(t, formatted, "select true as sampled from s5_orientation_boundaries")
	require.Contains(t, formatted, "s5_orientation_metrics as materialized")
	require.Contains(t, formatted, "s5_orientation_decision as materialized")
	require.Contains(t, formatted, "as would_select_reverse")
	require.Contains(t, formatted, "s5_orientation_shadow_forward as materialized")
	require.Contains(t, formatted, "s5_orientation_shadow_reverse as materialized")
	require.Contains(t, formatted, "s5_orientation_shadow_selection as materialized")
	require.Contains(t, formatted, "s5_orientation_executed_incumbent as materialized")
	require.Contains(t, formatted, "record_traversal_runtime_attestation_v1('EXPANSION-STEPWISE-FORWARD', 'shadow_incumbent', false)")
	require.Contains(t, formatted, "from s5_orientation_executed_incumbent join lateral")
	require.Contains(t, formatted, "s5_orientation_executed_incumbent.executed offset 0")
	require.Contains(t, formatted, "limit 513")
	require.Contains(t, formatted, "limit 16385")
	require.Contains(t, formatted, "offset 512 limit 1")
	require.Contains(t, formatted, "offset 16384 limit 1")
	require.NotContains(t, formatted, "s5_orientation_states")
	require.NotContains(t, formatted, "s5_orientation_reverse(boundary_id")
	require.NotContains(t, formatted, "limit 4097")
	forwardDegreeProjection := regexp.MustCompile(`(?s)s5_orientation_forward_degree_probe as materialized \(select (.*?) from s5_orientation_root_probe`).FindStringSubmatch(formatted)
	require.Len(t, forwardDegreeProjection, 2)
	require.Equal(t, "true as sampled", forwardDegreeProjection[1])
	reverseDegreeProjection := regexp.MustCompile(`(?s)s5_orientation_reverse_degree_probe as materialized \(select (.*?) from s5_orientation_boundaries`).FindStringSubmatch(formatted)
	require.Len(t, reverseDegreeProjection, 2)
	require.Equal(t, "true as sampled", reverseDegreeProjection[1])
	suffixProjection := regexp.MustCompile(`(?s)s5_orientation_suffix_probe as materialized \(select (.*?) from s5_orientation_root_presence`).FindStringSubmatch(formatted)
	require.Len(t, suffixProjection, 2)
	require.Equal(t, "n1.id as boundary_id", suffixProjection[1])

	require.Len(t, translation.Optimization.LoweringPlan.ExpansionSearchStrategy, 1)
	decision := translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0]
	require.Equal(t, optimize.ExpansionSearchStepwiseForward, decision.SelectedStrategy)
	require.Equal(t, optimize.ExpansionSearchPolicyOrientationProbeV1, decision.EmittedPolicy)
	require.Equal(t, []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchStepwiseForward}, decision.EmittedCandidates)
	require.Equal(t, "shadow_tool", decision.SelectionMode)
	require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV1), decision.SelectorVersion)
	require.Empty(t, decision.FallbackReason)

	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy, decision.Target)
	require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV1), outcome.EmittedPolicy)
	require.Equal(t, []string{string(optimize.ExpansionSearchStepwiseForward)}, outcome.EmittedCandidates)
	require.Equal(t, string(optimize.ExpansionSearchStepwiseForward), outcome.Selected)
	require.Equal(t, "inline_statement", outcome.ExecutionBoundary)
	require.Empty(t, outcome.Applied)
	require.Empty(t, outcome.SkipReason)
}

// TestSuffixOrientationShadowIsParameterStable verifies suffix orientation shadow is parameter stable behavior.
func TestSuffixOrientationShadowIsParameterStable(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)
	translate := func(rootKey string) string {
		translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
			"root_key": rootKey,
		}, DefaultGraphID, ToolOptions{EnableExpansionOrientationShadow: true})
		require.NoError(t, err)
		formatted, err := Translated(translation)
		require.NoError(t, err)
		return formatted
	}

	first := translate("shadow-root-a")
	second := translate("shadow-root-b")
	require.Equal(t, first, second)
	require.Contains(t, first, "@pi0::text")
}

// TestGuardedSuffixOrientationSQLIsParameterStable verifies guarded suffix orientation sql is parameter stable behavior.
func TestGuardedSuffixOrientationSQLIsParameterStable(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)
	translate := func(rootKey string) string {
		translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
			"root_key": rootKey,
		}, DefaultGraphID, ToolOptions{EnableExpansionOrientationTournament: true})
		require.NoError(t, err)
		formatted, err := Translated(translation)
		require.NoError(t, err)
		return formatted
	}

	first := translate("root-a")
	second := translate("root-b")
	require.Equal(t, first, second)
	require.Contains(t, first, "@pi0::text")
}

// TestGuardedSuffixOrientationAlignsSupportedOutputShapes verifies guarded suffix orientation aligns supported output shapes behavior.
func TestGuardedSuffixOrientationAlignsSupportedOutputShapes(t *testing.T) {
	for _, testCase := range []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// projection retains the projection while anonymous record is assembled or evaluated.
		projection string
		// expected retains the expected while anonymous record is assembled or evaluated.
		expected string
	}{
		{
			name:       "endpoint IDs",
			projection: "id(head), id(terminal)",
			expected:   `select s5.n2 as "id(head)", s5.n4 as "id(terminal)"`,
		},
		{
			name:       "ordered path IDs",
			projection: "length(path)",
			expected:   `as "length(path)"`,
		},
		{
			name:       "full path",
			projection: "path",
			expected:   "ordered_edge_ids_to_path",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
				MATCH (root:ExpansionRoot)
				WHERE root.root_key = $root_key
				MATCH path = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
				RETURN `+testCase.projection)
			require.NoError(t, err)
			translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
				"root_key": "output-root",
			}, DefaultGraphID, ToolOptions{EnableExpansionOrientationTournament: true})
			require.NoError(t, err)
			formatted, err := Translated(translation)
			require.NoError(t, err)
			require.Contains(t, formatted, testCase.expected)
			require.Equal(t, optimize.ExpansionSearchPolicyOrientationProbeV1, translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0].EmittedPolicy)
		})
	}
}

// TestProductionFixedSuffixTranslationRemainsIncumbent verifies production fixed suffix translation remains incumbent behavior.
func TestProductionFixedSuffixTranslationRemainsIncumbent(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)
	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "production-root",
	}, DefaultGraphID)
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.NotContains(t, formatted, "_orientation_")
	require.Contains(t, formatted, "s2(root_id, next_id, depth, satisfied, is_cycle, path)")

	decision := translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0]
	require.Equal(t, optimize.ExpansionSearchStepwiseForward, decision.SelectedStrategy)
	require.Empty(t, decision.EmittedPolicy)
	require.Equal(t, []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchStepwiseForward}, decision.EmittedCandidates)
	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy, decision.Target)
	require.Equal(t, "inline_statement", outcome.ExecutionBoundary)
}

// TestGuardedSuffixOrientationUsesOnlyTargetGraphRelations verifies guarded suffix orientation uses only target graph relations behavior.
func TestGuardedSuffixOrientationUsesOnlyTargetGraphRelations(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)
	translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "graph-scoped-root",
	}, 42, ToolOptions{EnableExpansionOrientationTournament: true})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)

	require.Contains(t, formatted, "node_42")
	require.Contains(t, formatted, "edge_42")
	require.Contains(t, formatted, "ordered_edge_ids_to_path(42,")
	require.NotRegexp(t, regexp.MustCompile(`(?i)(from|join) (node|edge)(?:\s|;)`), formatted)
}

// TestExpansionOrientationTournamentRejectsConflictingForceWithoutMutation verifies expansion orientation tournament rejects conflicting force without mutation behavior.
func TestExpansionOrientationTournamentRejectsConflictingForceWithoutMutation(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)
	plan, err := optimize.Optimize(regularQuery)
	require.NoError(t, err)
	before := append([]optimize.ExpansionSearchStrategyDecision(nil), plan.LoweringPlan.ExpansionSearchStrategy...)

	err = applyToolOptions(&plan, ToolOptions{
		EnableExpansionOrientationTournament: true,
		ForceExpansionSearchStrategy:         optimize.ExpansionSearchSuffixSeededReverse,
	})
	require.ErrorContains(t, err, "mutually exclusive")
	require.Equal(t, before, plan.LoweringPlan.ExpansionSearchStrategy)
}

// TestExpansionOrientationShadowRejectsConflictingModesWithoutMutation verifies expansion orientation shadow rejects conflicting modes without mutation behavior.
func TestExpansionOrientationShadowRejectsConflictingModesWithoutMutation(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)
	plan, err := optimize.Optimize(regularQuery)
	require.NoError(t, err)
	before := append([]optimize.ExpansionSearchStrategyDecision(nil), plan.LoweringPlan.ExpansionSearchStrategy...)

	for _, options := range []ToolOptions{
		{
			EnableExpansionOrientationTournament: true,
			EnableExpansionOrientationShadow:     true,
		},
		{
			EnableExpansionOrientationShadow: true,
			ForceExpansionSearchStrategy:     optimize.ExpansionSearchSuffixSeededReverse,
		},
	} {
		err := applyToolOptions(&plan, options)
		require.ErrorContains(t, err, "mutually exclusive")
		require.Equal(t, before, plan.LoweringPlan.ExpansionSearchStrategy)
	}
}

// TestExpansionOrientationPolicyRequiresSupportedEnabledMode verifies expansion orientation policy requires supported enabled mode behavior.
func TestExpansionOrientationPolicyRequiresSupportedEnabledMode(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)
	plan, err := optimize.Optimize(regularQuery)
	require.NoError(t, err)
	before := append([]optimize.ExpansionSearchStrategyDecision(nil), plan.LoweringPlan.ExpansionSearchStrategy...)

	err = applyToolOptions(&plan, ToolOptions{ExpansionOrientationPolicy: optimize.ExpansionSearchPolicyOrientationProbeV2})
	require.ErrorContains(t, err, "requires tournament or shadow mode")
	require.Equal(t, before, plan.LoweringPlan.ExpansionSearchStrategy)

	err = applyToolOptions(&plan, ToolOptions{
		ExpansionOrientationPolicy:           optimize.ExpansionSearchPolicy("orientation-probe-v3"),
		EnableExpansionOrientationTournament: true,
	})
	require.ErrorContains(t, err, "unsupported expansion orientation policy")
	require.Equal(t, before, plan.LoweringPlan.ExpansionSearchStrategy)
}

// TestExpansionOrientationShadowRequiresExactlyOneEligibleTarget verifies expansion orientation shadow requires exactly one eligible target behavior.
func TestExpansionOrientationShadowRequiresExactlyOneEligibleTarget(t *testing.T) {
	plan := optimize.Plan{LoweringPlan: optimize.LoweringPlan{
		ExpansionSearchStrategy: []optimize.ExpansionSearchStrategyDecision{
			{
				Family:               "fixed_suffix_expansion",
				CandidateStrategy:    optimize.ExpansionSearchSuffixSeededReverse,
				SelectedStrategy:     optimize.ExpansionSearchStepwiseForward,
				StructurallyEligible: true,
				StaticallyEligible:   true,
				EmittedCandidates:    []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchStepwiseForward},
			},
			{
				Family:               "fixed_suffix_expansion",
				CandidateStrategy:    optimize.ExpansionSearchSuffixSeededReverse,
				SelectedStrategy:     optimize.ExpansionSearchStepwiseForward,
				StructurallyEligible: true,
				StaticallyEligible:   true,
				EmittedCandidates:    []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchStepwiseForward},
			},
		},
	}}
	before := append([]optimize.ExpansionSearchStrategyDecision(nil), plan.LoweringPlan.ExpansionSearchStrategy...)

	err := applyExpansionOrientationShadow(&plan)
	require.ErrorContains(t, err, "matched 2 structurally eligible fixed-suffix targets; expected exactly one")
	require.Equal(t, before, plan.LoweringPlan.ExpansionSearchStrategy)
}

// TestExpansionOrientationTournamentRequiresExactlyOneEligibleTarget verifies expansion orientation tournament requires exactly one eligible target behavior.
func TestExpansionOrientationTournamentRequiresExactlyOneEligibleTarget(t *testing.T) {
	plan := optimize.Plan{LoweringPlan: optimize.LoweringPlan{
		ExpansionSearchStrategy: []optimize.ExpansionSearchStrategyDecision{
			{
				Family:               "fixed_suffix_expansion",
				CandidateStrategy:    optimize.ExpansionSearchSuffixSeededReverse,
				SelectedStrategy:     optimize.ExpansionSearchStepwiseForward,
				StructurallyEligible: true,
				StaticallyEligible:   true,
				EmittedCandidates:    []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchStepwiseForward},
				FallbackReason:       optimize.ExpansionSearchFallbackTournamentUnqualified,
			},
			{
				Family:               "fixed_suffix_expansion",
				CandidateStrategy:    optimize.ExpansionSearchSuffixSeededReverse,
				SelectedStrategy:     optimize.ExpansionSearchStepwiseForward,
				StructurallyEligible: true,
				StaticallyEligible:   true,
				EmittedCandidates:    []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchStepwiseForward},
				FallbackReason:       optimize.ExpansionSearchFallbackTournamentUnqualified,
			},
		},
	}}
	before := append([]optimize.ExpansionSearchStrategyDecision(nil), plan.LoweringPlan.ExpansionSearchStrategy...)

	err := applyExpansionOrientationTournament(&plan)
	require.ErrorContains(t, err, "matched 2 structurally eligible fixed-suffix targets; expected exactly one")
	require.Equal(t, before, plan.LoweringPlan.ExpansionSearchStrategy)
}

// TestExpansionOrientationTournamentRejectsNonInitialVariableRegion verifies expansion orientation tournament rejects non initial variable region behavior.
func TestExpansionOrientationTournamentRejectsNonInitialVariableRegion(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (root:ExpansionRoot)
		WHERE root.root_key = $root_key
		MATCH ()-[:Prefix]->(root)-[:Expand*0..16]->()-[:EnterSuffix]->(:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(:SuffixTerminal)
		RETURN id(root)
	`)
	require.NoError(t, err)
	plan, err := optimize.Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 1)
	require.False(t, plan.LoweringPlan.ExpansionSearchStrategy[0].StructurallyEligible)
	require.NotEqual(t, "fixed_suffix_expansion", plan.LoweringPlan.ExpansionSearchStrategy[0].Family)
	require.ErrorContains(t, applyExpansionOrientationTournament(&plan), "has no structurally eligible fixed-suffix target")
}
