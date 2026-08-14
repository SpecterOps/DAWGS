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

// TestSuffixReverseGuardRejectsEndpointOnlyObservation verifies that endpoint
// cases remain on the incumbent and cannot be silently enrolled by tooling.
func TestSuffixReverseGuardRejectsEndpointOnlyObservation(t *testing.T) {
	query := strings.Replace(guardedSuffixOrientationQuery, "RETURN path", "RETURN id(terminal)", 1)
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), query)
	require.NoError(t, err)
	_, err = TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "suffix-guard-endpoint",
	}, DefaultGraphID, ToolOptions{EnableExpansionSuffixReverseGuard: true})
	require.ErrorContains(t, err, "no statically eligible full-path fixed-suffix target")

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
	_, err = TranslateForTool(context.Background(), regularQuery, optimizerSafetyKindMapper(), map[string]any{
		"root_key": "suffix-guard-mutation",
	}, DefaultGraphID, ToolOptions{EnableExpansionSuffixReverseGuard: true})
	require.ErrorContains(t, err, "no statically eligible full-path fixed-suffix target")
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
		{SuffixReverseGuardSuffixRowLimit: 1},
		{EnableExpansionSuffixReverseGuard: true, SuffixReverseGuardSuffixRowLimit: -1},
		{EnableExpansionSuffixReverseGuard: true, SuffixReverseGuardStateLimit: -1},
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
