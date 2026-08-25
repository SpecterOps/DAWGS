// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

func translateSuffixReverseGuard(t *testing.T, query string, options ToolOptions) (Result, string) {
	t.Helper()
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), query)
	require.NoError(t, err)
	translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "suffix-guard-root",
	}, DefaultGraphID, options)
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	return translation, formatted
}

// TestSuffixReverseGuardEmitsStaticFullPathDualArm verifies that the tool-only
// policy has a distinct identity, emits no orientation probes, and retains an
// exact marker-gated fallback in the same statement.
func TestSuffixReverseGuardEmitsStaticFullPathDualArm(t *testing.T) {
	translation, formatted := translateSuffixReverseGuard(t, guardedSuffixOrientationQuery, ToolOptions{
		EnableExpansionSuffixReverseGuard: true,
	})

	require.NotNil(t, translation.Optimization.LoweringPlan)
	require.Len(t, translation.Optimization.LoweringPlan.ExpansionSearchStrategy, 1)
	decision := translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0]
	require.Equal(t, optimize.ExpansionSearchPolicySuffixReverseGuardV1, decision.PlannedPolicy)
	require.Equal(t, optimize.ExpansionSearchPolicySuffixReverseGuardV1, decision.EmittedPolicy)
	require.Equal(t, optimize.ExpansionSearchSelectorFixedSuffixPathV1, decision.SelectorVersion)
	require.Equal(t, optimize.ExpansionSearchObservationFullPath, decision.ObservationMode)
	require.Equal(t, "guarded_tool", decision.SelectionMode)
	require.Equal(t, optimize.ExpansionSearchExecutionBoundaryGuardedDualArm, decision.ExecutionBoundary)
	require.Equal(t, []optimize.ExpansionSearchStrategy{
		optimize.ExpansionSearchSuffixSeededReverse,
		optimize.ExpansionSearchStepwiseForward,
	}, decision.EmittedCandidates)
	require.Equal(t, optimize.ExpansionSearchProbeCaps{
		ReverseSeedRowLimit: optimize.ExpansionSearchSuffixReverseGuardSuffixRowLimit,
	}, decision.ProbeCaps)
	require.Equal(t, optimize.ExpansionSearchAdmission{
		StateLimit:             optimize.ExpansionSearchSuffixReverseGuardStateLimit,
		RequiresCompleteProbes: true,
		FallbackStrategy:       optimize.ExpansionSearchStepwiseForward,
	}, decision.Admission)
	require.Equal(t, optimize.ExpansionSearchSuffixReverseGuardStateLimit, decision.StateLimit)
	require.Equal(t, optimize.ExpansionSearchStepwiseForward, decision.FallbackStrategy)
	require.Contains(t, decision.EligibilityFacts, optimize.ExpansionSearchEligibilityFact{
		Name:     "full_path_observation",
		Eligible: true,
	})

	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy, decision.Target)
	require.Equal(t, string(optimize.ExpansionSearchPolicySuffixReverseGuardV1), outcome.PlannedPolicy)
	require.Equal(t, string(optimize.ExpansionSearchPolicySuffixReverseGuardV1), outcome.EmittedPolicy)
	require.Equal(t, optimize.ExpansionSearchSelectorFixedSuffixPathV1, outcome.SelectorVersion)

	for _, cte := range []string{
		"_suffix_guard_root_presence",
		"_suffix_guard_suffix_probe",
		"_suffix_guard_boundaries",
		"_suffix_guard_states",
		"_suffix_guard_admission",
		"_suffix_guard_decision",
		"_suffix_guard_candidate_marker",
		"_suffix_guard_fallback_marker",
		"_suffix_guard_candidate_body",
		"_suffix_guard_fallback_body",
	} {
		require.Contains(t, formatted, cte)
	}
	require.Equal(t, 1, strings.Count(formatted, "record_requested_traversal_runtime_attestation_v1("))
	require.Contains(t, formatted, "EXPANSION-STEPWISE-FORWARD")
	require.Contains(t, formatted, "EXPANSION-SUFFIX-SEEDED-REVERSE")
	require.Contains(t, formatted, "suffix_seeded_reverse")
	require.Contains(t, formatted, "exists (select 1")
	require.Contains(t, formatted, "generate_subscripts")
	require.Contains(t, formatted, "union all")
	require.NotContains(t, formatted, "_orientation_")
	require.NotContains(t, formatted, "forward_degree")
	require.NotContains(t, formatted, "reverse_degree")
	require.NotContains(t, formatted, "would_select")
}

// TestSuffixReverseGuardToolCapsAreExplicit verifies that diagnostic cap
// overrides are copied into both lowering metadata and cap+1 SQL sentinels.
func TestSuffixReverseGuardToolCapsAreExplicit(t *testing.T) {
	translation, formatted := translateSuffixReverseGuard(t, guardedSuffixOrientationQuery, ToolOptions{
		EnableExpansionSuffixReverseGuard: true,
		SuffixReverseGuardSuffixRowLimit:  7,
		SuffixReverseGuardStateLimit:      11,
	})
	decision := translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0]
	require.Equal(t, int64(7), decision.ProbeCaps.ReverseSeedRowLimit)
	require.Equal(t, int64(11), decision.Admission.StateLimit)
	require.Contains(t, formatted, "limit 8")
	require.Contains(t, formatted, "offset 7")
	require.Contains(t, formatted, "limit 12")
	require.Contains(t, formatted, "offset 11")
}

// TestSuffixReverseRetryEmitsOnlyBoundedCandidate verifies the P1 development
// statement contains no incumbent body and reports a transaction-local status
// before any buffered row can be published.
func TestSuffixReverseRetryEmitsOnlyBoundedCandidate(t *testing.T) {
	translation, formatted := translateSuffixReverseGuard(t, guardedSuffixOrientationQuery, ToolOptions{
		EnableExpansionSuffixReverseRetry: true,
	})
	decision := translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0]
	require.Equal(t, optimize.ExpansionSearchPolicySuffixReverseRetryV1, decision.PlannedPolicy)
	require.Equal(t, optimize.ExpansionSearchPolicySuffixReverseRetryV1, decision.EmittedPolicy)
	require.Equal(t, "transaction_retry_tool", decision.SelectionMode)
	require.Equal(t, optimize.ExpansionSearchExecutionBoundaryTransactionRetry, decision.ExecutionBoundary)
	require.Equal(t, optimize.ExpansionSearchSuffixSeededReverse, decision.SelectedStrategy)
	require.Equal(t, []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchSuffixSeededReverse}, decision.EmittedCandidates)
	require.Equal(t, optimize.ExpansionSearchAdmission{
		StateLimit:             optimize.ExpansionSearchSuffixReverseGuardStateLimit,
		OutputRowLimit:         optimize.ExpansionSearchSuffixReverseRetryOutputRowLimit,
		OutputBytesLimit:       optimize.ExpansionSearchSuffixReverseRetryOutputBytesLimit,
		RequiresCompleteProbes: true,
		FallbackStrategy:       optimize.ExpansionSearchStepwiseForward,
	}, decision.Admission)
	require.Contains(t, formatted, "set_config('dawgs.suffix_reverse_retry_status'")
	require.Contains(t, formatted, "forward_retry_suffix_overflow")
	require.Contains(t, formatted, "forward_retry_state_overflow")
	require.Contains(t, formatted, "reverse_complete")
	require.Contains(t, formatted, "limit 4097")
	require.Contains(t, formatted, "_suffix_guard_candidate_body")
	require.NotContains(t, formatted, "_suffix_guard_fallback_body")
	require.NotContains(t, formatted, "_suffix_guard_fallback_rows")
}

func TestProductionTopologyFixedSuffixEmitsOneGuardedCandidate(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)
	translation, err := TranslateWithProductionOptions(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "suffix-guard-root",
	}, DefaultGraphID, ProductionOptions{
		EnableTopologyFixedSuffix: true,
		TopologyFixedSuffixCaps: &ProductionFixedSuffixCaps{
			SuffixRowLimit: 7, StateLimit: 11, OutputRowLimit: 13, OutputBytesLimit: 17,
		},
		SelectorVersion: string(optimize.ExpansionSearchPolicyTopologyFixedSuffixV1),
	})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	decision := translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0]
	require.Equal(t, optimize.ExpansionSearchPolicyTopologyFixedSuffixV1, decision.PlannedPolicy)
	require.Equal(t, optimize.ExpansionSearchPolicyTopologyFixedSuffixV1, decision.EmittedPolicy)
	require.Equal(t, "production_canary", decision.SelectionMode)
	require.Equal(t, optimize.ExpansionSearchExecutionBoundaryTransactionRetry, decision.ExecutionBoundary)
	require.Equal(t, []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchSuffixSeededReverse}, decision.EmittedCandidates)
	require.Contains(t, formatted, "set_config('dawgs.suffix_reverse_retry_status'")
	require.NotContains(t, formatted, "_suffix_guard_fallback_body")
}

func TestProductionTopologyFixedSuffixFirstUseEmitsRetryCandidate(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)
	translation, err := TranslateWithProductionOptions(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "suffix-guard-root",
	}, DefaultGraphID, ProductionOptions{
		EnableTopologyFixedSuffix: true,
		TopologyFixedSuffixCaps: &ProductionFixedSuffixCaps{
			SuffixRowLimit: 7, StateLimit: 11, OutputRowLimit: 13, OutputBytesLimit: 17,
		},
		SelectorVersion: string(optimize.ExpansionSearchPolicyTopologyFixedSuffixFirstUseV1),
	})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	decision := translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0]
	require.Equal(t, optimize.ExpansionSearchPolicyTopologyFixedSuffixFirstUseV1, decision.PlannedPolicy)
	require.Equal(t, optimize.ExpansionSearchPolicyTopologyFixedSuffixFirstUseV1, decision.EmittedPolicy)
	require.Equal(t, optimize.ExpansionSearchExecutionBoundaryTransactionRetry, decision.ExecutionBoundary)
	require.Contains(t, formatted, "set_config('dawgs.suffix_reverse_retry_status'")
	require.NotContains(t, formatted, "_suffix_guard_fallback_body")
}

func TestSuffixReverseRetryLowersEveryIndependentFixedSuffixTarget(t *testing.T) {
	plan := &optimize.Plan{LoweringPlan: optimize.LoweringPlan{ExpansionSearchStrategy: []optimize.ExpansionSearchStrategyDecision{
		{Family: "fixed_suffix_expansion", CandidateStrategy: optimize.ExpansionSearchSuffixSeededReverse, StructurallyEligible: true, StaticallyEligible: true, ObservationMode: optimize.ExpansionSearchObservationFullPath},
		{Family: "fixed_suffix_expansion", CandidateStrategy: optimize.ExpansionSearchSuffixSeededReverse, StructurallyEligible: true, StaticallyEligible: true, ObservationMode: optimize.ExpansionSearchObservationFullPath},
	}}}
	require.NoError(t, applyExpansionSuffixReverseRetryPolicy(plan, 0, 0, 0, 0))
	for _, decision := range plan.LoweringPlan.ExpansionSearchStrategy {
		require.Equal(t, optimize.ExpansionSearchSuffixSeededReverse, decision.SelectedStrategy)
		require.Equal(t, "transaction_retry_tool", decision.SelectionMode)
		require.Equal(t, optimize.ExpansionSearchPolicySuffixReverseRetryV1, decision.EmittedPolicy)
	}
}

// TestSuffixRouteComponentEmitsOneExactReverseStatement verifies the new
// default-off preflight arm has no probe, fallback, retry, or production-policy
// identity while retaining one runtime receipt for diagnostic attestation.
func TestSuffixRouteComponentEmitsOneExactReverseStatement(t *testing.T) {
	translation, formatted := translateSuffixReverseGuard(t, guardedSuffixOrientationQuery, ToolOptions{
		EnableExpansionSuffixRouteComponent: true,
	})
	decision := translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0]
	require.Empty(t, decision.PlannedPolicy)
	require.Empty(t, decision.EmittedPolicy)
	require.Equal(t, "component_tool", decision.SelectionMode)
	require.Equal(t, optimize.ExpansionSearchSelectorSuffixRouteComponentV1, decision.SelectorVersion)
	require.Equal(t, optimize.ExpansionSearchExecutionBoundaryInlineStatement, decision.ExecutionBoundary)
	require.Equal(t, optimize.ExpansionSearchSuffixSeededReverse, decision.SelectedStrategy)
	require.Equal(t, []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchSuffixSeededReverse}, decision.EmittedCandidates)
	require.Empty(t, decision.ProbeCaps)
	require.Empty(t, decision.Admission)
	require.Empty(t, decision.FallbackStrategy)

	outcome := requireTraversalTargetOutcome(t, translation.Optimization, optimize.LoweringExpansionSearchStrategy, decision.Target)
	require.Equal(t, "component_tool", outcome.SelectionMode)
	require.Equal(t, optimize.ExpansionSearchSelectorSuffixRouteComponentV1, outcome.SelectorVersion)
	require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), outcome.Selected)
	require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), outcome.Applied)
	require.Empty(t, outcome.EmittedPolicy)

	require.Contains(t, formatted, "_suffix_seeded_component_receipt")
	require.Contains(t, formatted, "record_requested_traversal_runtime_attestation_v1('suffix_route_component', false, 'EXPANSION-SUFFIX-SEEDED-REVERSE')")
	require.Contains(t, formatted, "EXPANSION-SUFFIX-SEEDED-REVERSE")
	require.NotContains(t, formatted, "_suffix_guard_")
	require.NotContains(t, formatted, "_orientation_")
	require.NotContains(t, formatted, "EXPANSION-STEPWISE-FORWARD")
	require.NotContains(t, formatted, "forward_retry_")
}

// TestSuffixRouteComponentAdmitsEndpointObservation verifies the direct
// component measures both output shapes while retry/guard remain path-only.
func TestSuffixRouteComponentAdmitsEndpointObservation(t *testing.T) {
	query := strings.Replace(guardedSuffixOrientationQuery, "RETURN path", "RETURN id(terminal)", 1)
	translation, formatted := translateSuffixReverseGuard(t, query, ToolOptions{
		EnableExpansionSuffixRouteComponent: true,
	})
	decision := translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0]
	require.Equal(t, optimize.ExpansionSearchObservationEndpointIDs, decision.ObservationMode)
	require.Equal(t, optimize.ExpansionSearchSuffixSeededReverse, decision.SelectedStrategy)
	require.Contains(t, formatted, "_suffix_seeded_component_receipt")
}

// TestSuffixReverseGuardRejectsEndpointOnlyObservation verifies that endpoint
// cases remain on the incumbent and cannot be silently enrolled by tooling.
func TestSuffixReverseGuardRejectsEndpointOnlyObservation(t *testing.T) {
	query := strings.Replace(guardedSuffixOrientationQuery, "RETURN path", "RETURN id(terminal)", 1)
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), query)
	require.NoError(t, err)
	for name, options := range map[string]ToolOptions{
		"guard": {EnableExpansionSuffixReverseGuard: true},
		"retry": {EnableExpansionSuffixReverseRetry: true},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
				"root_key": "suffix-guard-endpoint",
			}, DefaultGraphID, options)
			require.ErrorContains(t, err, "statically eligible full-path fixed-suffix target")
		})
	}

	incumbent, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "suffix-guard-endpoint",
	}, DefaultGraphID)
	require.NoError(t, err)
	formatted, err := Translated(incumbent)
	require.NoError(t, err)
	require.NotContains(t, formatted, "_suffix_guard_")
}

// TestSuffixReverseGuardRejectsMutation verifies that the static full-path
// envelope does not override the optimizer's read-only qualification.
func TestSuffixReverseGuardRejectsMutation(t *testing.T) {
	query := strings.Replace(guardedSuffixOrientationQuery, "RETURN path", "CREATE (created) RETURN path", 1)
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), query)
	require.NoError(t, err)
	for name, options := range map[string]ToolOptions{
		"guard": {EnableExpansionSuffixReverseGuard: true},
		"retry": {EnableExpansionSuffixReverseRetry: true},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
				"root_key": "suffix-guard-mutation",
			}, DefaultGraphID, options)
			require.ErrorContains(t, err, "statically eligible full-path fixed-suffix target")
		})
	}
}

// TestSuffixReverseGuardTemplateIsParameterStable verifies that tool policy
// selection and cap literals do not specialize SQL to runtime parameter data.
func TestSuffixReverseGuardTemplateIsParameterStable(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)
	translate := func(rootKey string) string {
		translation, err := TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
			"root_key": rootKey,
		}, DefaultGraphID, ToolOptions{EnableExpansionSuffixReverseGuard: true})
		require.NoError(t, err)
		formatted, err := Translated(translation)
		require.NoError(t, err)
		return formatted
	}
	require.Equal(t, translate("root-a"), translate("root-b"))
}

// TestSuffixReverseGuardOptionsAreIsolated verifies the new policy cannot be
// combined with unrelated experimental selectors and rejects invalid caps.
func TestSuffixReverseGuardOptionsAreIsolated(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)
	plan, err := optimize.Optimize(regularQuery)
	require.NoError(t, err)

	for _, options := range []ToolOptions{
		{
			EnableExpansionSuffixReverseGuard:    true,
			EnableExpansionOrientationTournament: true,
		},
		{
			EnableExpansionSuffixReverseGuard: true,
			ForceExpansionSearchStrategy:      optimize.ExpansionSearchSuffixSeededReverse,
		},
		{
			EnableExpansionSuffixReverseGuard: true,
			EnableExpansionSuffixReverseRetry: true,
		},
		{SuffixReverseGuardSuffixRowLimit: 1},
		{EnableExpansionSuffixReverseGuard: true, SuffixReverseGuardSuffixRowLimit: -1},
		{EnableExpansionSuffixReverseGuard: true, SuffixReverseGuardStateLimit: -1},
		{EnableExpansionSuffixReverseRetry: true, SuffixReverseRetryOutputRowLimit: -1},
		{EnableExpansionSuffixReverseRetry: true, SuffixReverseRetryOutputBytesLimit: -1},
	} {
		planCopy := plan
		require.Error(t, applyToolOptions(&planCopy, options))
	}
}

// TestProductionTranslationDoesNotEmitSuffixReverseGuard verifies that the
// zero-value production path remains unchanged by this tool-only feature.
func TestProductionTranslationDoesNotEmitSuffixReverseGuard(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), guardedSuffixOrientationQuery)
	require.NoError(t, err)
	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "suffix-guard-production-default",
	}, DefaultGraphID)
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.NotContains(t, formatted, "_suffix_guard_")
	require.Empty(t, translation.Optimization.LoweringPlan.ExpansionSearchStrategy[0].EmittedPolicy)
}
