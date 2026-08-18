// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

// TestPostgresTraversalTelemetryCompletesBidirectionalCandidateIdentityChain verifies postgres traversal telemetry completes bidirectional candidate identity chain behavior.
func TestPostgresTraversalTelemetryCompletesBidirectionalCandidateIdentityChain(t *testing.T) {
	telemetry := bidirectionalCaseTelemetry(t, TraversalTelemetryLevelDiagnostic)
	require.Equal(t, TraversalTelemetryCounterStatusHiddenUnavailable, telemetry.Diagnostic.CounterStatus)

	document := validBidirectionalDiagnosticDocument(telemetry.Diagnostic.InvocationID)
	require.NoError(t, applyBidirectionalTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "9123"))
	require.NoError(t, telemetry.Validate())

	require.Equal(t, "SP-B2-C-MIN-LEVEL-D", telemetry.Summary.RequestedIdentity)
	require.Equal(t, "SP-B2-C-MIN-LEVEL-D", telemetry.Summary.RuntimeIdentity)
	require.Equal(t, "SP-B2-C-MIN-LEVEL-D", telemetry.Summary.AppliedIdentity)
	require.Equal(t, "bidirectional_search", telemetry.Summary.RuntimeBranch)
	require.False(t, *telemetry.Summary.FallbackExecuted)
	require.Equal(t, TraversalTelemetryCounterStatusComplete, telemetry.Diagnostic.CounterStatus)
	require.Contains(t, telemetry.Diagnostic.RequiredFamilies, TraversalTelemetryFamilyWorkspace)
	require.False(t, *telemetry.Diagnostic.TimedSample)
	require.Equal(t, int64(7), *telemetry.Diagnostic.Counters.ShortestPath.CandidateEdges)
	require.Equal(t, int64(4), *telemetry.Diagnostic.Counters.ShortestPath.PredecessorPeak)
	require.Equal(t, int64(4), *telemetry.Diagnostic.Counters.ShortestPath.Levels[0].PredecessorRows)
	require.NotNil(t, telemetry.Diagnostic.Counters.Workspace)
	observed := traversalNumericObservations(telemetry.Diagnostic.Counters)
	require.Equal(t, int64(6), observed["state_rows"])
	require.Equal(t, int64(3), observed["frontier_rows"])
	require.Equal(t, int64(3), observed["queue_rows"])
	require.Equal(t, int64(4), observed["predecessor_rows"])
}

// TestPostgresTraversalTelemetryRebindsRuntimeIdentityOnExactFallback verifies postgres traversal telemetry rebinds runtime identity on exact fallback behavior.
func TestPostgresTraversalTelemetryRebindsRuntimeIdentityOnExactFallback(t *testing.T) {
	telemetry := bidirectionalCaseTelemetry(t, TraversalTelemetryLevelDiagnostic)
	document := validBidirectionalDiagnosticDocument(telemetry.Diagnostic.InvocationID)
	document.RuntimeBranch = "exact_s4_fallback"
	document.Overflowed = traversalTelemetryPointer(true)
	document.FallbackExecuted = traversalTelemetryPointer(true)
	document.Calls[0].RuntimeBranch = "exact_s4_fallback"
	document.Calls[0].Overflowed = traversalTelemetryPointer(true)
	document.Calls[0].FallbackExecuted = traversalTelemetryPointer(true)

	require.NoError(t, applyBidirectionalTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "9123"))
	require.NoError(t, telemetry.Validate())

	require.Equal(t, "SP-S4-C-D", telemetry.Summary.RuntimeIdentity)
	require.Equal(t, "SP-S4-C-D", telemetry.Summary.AppliedIdentity)
	require.Equal(t, "SP-S4-C-D", telemetry.Summary.FallbackIdentity)
	require.True(t, *telemetry.Summary.Overflow)
	require.True(t, *telemetry.Summary.FallbackExecuted)
	require.Contains(t, telemetry.Summary.PlannedIdentities, "SP-S4-C-D")
	require.Equal(t, TraversalTelemetryCounterStatusHiddenUnavailable, telemetry.Diagnostic.CounterStatus)
	require.Contains(t, telemetry.Diagnostic.IncompleteReasons[0], "S4 fallback")
}

// TestPostgresTraversalTelemetryRejectsInvocationConnectionAndCapMismatch verifies postgres traversal telemetry rejects invocation connection and cap mismatch behavior.
func TestPostgresTraversalTelemetryRejectsInvocationConnectionAndCapMismatch(t *testing.T) {
	telemetry := bidirectionalCaseTelemetry(t, TraversalTelemetryLevelDiagnostic)
	document := validBidirectionalDiagnosticDocument(telemetry.Diagnostic.InvocationID)

	err := applyBidirectionalTraversalDiagnostic(telemetry, document, "another-invocation", "9123")
	require.ErrorContains(t, err, "invocation identity")

	telemetry = bidirectionalCaseTelemetry(t, TraversalTelemetryLevelDiagnostic)
	document = validBidirectionalDiagnosticDocument(telemetry.Diagnostic.InvocationID)
	err = applyBidirectionalTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "different-backend")
	require.ErrorContains(t, err, "connection identity")

	telemetry = bidirectionalCaseTelemetry(t, TraversalTelemetryLevelDiagnostic)
	document = validBidirectionalDiagnosticDocument(telemetry.Diagnostic.InvocationID)
	document.FrontierLimit = traversalTelemetryPointer(int64(99))
	err = applyBidirectionalTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "9123")
	require.ErrorContains(t, err, "cap")

	telemetry = bidirectionalCaseTelemetry(t, TraversalTelemetryLevelDiagnostic)
	document = validBidirectionalDiagnosticDocument(telemetry.Diagnostic.InvocationID)
	document.Counters = nil
	err = applyBidirectionalTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "9123")
	require.ErrorContains(t, err, "counters are missing")
}

// TestPostgresTraversalTelemetryRequiresExactlyOneSingletonSearchCall verifies postgres traversal telemetry requires exactly one singleton search call behavior.
func TestPostgresTraversalTelemetryRequiresExactlyOneSingletonSearchCall(t *testing.T) {
	telemetry := bidirectionalCaseTelemetry(t, TraversalTelemetryLevelDiagnostic)
	document := validBidirectionalDiagnosticDocument(telemetry.Diagnostic.InvocationID)
	document.SearchCalls = traversalTelemetryPointer(int64(2))
	document.Calls = append(document.Calls, document.Calls[0])
	document.Calls[1].SearchID = traversalTelemetryPointer(int64(2))

	err := applyBidirectionalTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "9123")
	require.ErrorContains(t, err, "exactly one search call")
}

// TestPostgresTraversalTelemetryAcceptsSQLPreflightBranchesAndNoPathSentinel
// verifies the Go reader accepts the exact runtime branch spellings emitted by
// the compact SQL kernel, including its nil call/-1 aggregate no-path
// distance representation.
func TestPostgresTraversalTelemetryAcceptsSQLPreflightBranchesAndNoPathSentinel(t *testing.T) {
	for _, branch := range []string{
		"zero_hop_preflight",
		"one_hop_preflight",
		"two_hop_preflight",
		"preflight_no_path",
		"search_no_path",
	} {
		t.Run(branch, func(t *testing.T) {
			telemetry := bidirectionalCaseTelemetry(t, TraversalTelemetryLevelDiagnostic)
			document := validBidirectionalDiagnosticDocument(telemetry.Diagnostic.InvocationID)
			document.RuntimeBranch = branch
			document.Calls[0].RuntimeBranch = branch
			if branch == "preflight_no_path" || branch == "search_no_path" {
				document.Counters.FrozenDistance = traversalTelemetryPointer(int64(-1))
				document.Calls[0].FrozenDistance = nil
			}

			require.NoError(t, applyBidirectionalTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "9123"))
			require.NoError(t, telemetry.Validate())
			require.Equal(t, branch, telemetry.Summary.RuntimeBranch)
		})
	}
}

// TestPostgresTraversalTelemetryCapturesASPWorkAndWorkspaceButFailsClosedWithoutHydration verifies postgres traversal telemetry captures asp work and workspace but fails closed without hydration behavior.
func TestPostgresTraversalTelemetryCapturesASPWorkAndWorkspaceButFailsClosedWithoutHydration(t *testing.T) {
	telemetry := bidirectionalASPCaseTelemetry(t)
	document := validBidirectionalAllShortestDiagnosticDocument(telemetry.Diagnostic.InvocationID)

	require.NoError(t, applyBidirectionalAllShortestTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "9123"))
	require.NoError(t, telemetry.Validate())
	require.Equal(t, "ASP-B2-DAG-MIN-LEVEL", telemetry.Summary.RuntimeIdentity)
	require.Equal(t, TraversalTelemetryCounterStatusHiddenUnavailable, telemetry.Diagnostic.CounterStatus)
	require.Equal(t, []TraversalTelemetryFamily{
		TraversalTelemetryFamilyASP,
		TraversalTelemetryFamilyHydration,
		TraversalTelemetryFamilyWorkspace,
	}, telemetry.Diagnostic.RequiredFamilies)
	require.Equal(t, int64(13), *telemetry.Diagnostic.Counters.AllShortestPaths.EnumeratedCandidates)
	require.Equal(t, int64(384), *telemetry.Diagnostic.Counters.AllShortestPaths.OutputBytes)
	require.Nil(t, telemetry.Diagnostic.Counters.Hydration)
	require.NotNil(t, telemetry.Diagnostic.Counters.Workspace)
}

// TestPostgresTraversalTelemetryCompletesASPHydrationFromInvocationAndPlanEvidence verifies postgres traversal telemetry completes asp hydration from invocation and plan evidence behavior.
func TestPostgresTraversalTelemetryCompletesASPHydrationFromInvocationAndPlanEvidence(t *testing.T) {
	telemetry := bidirectionalASPCaseTelemetry(t)
	document := validBidirectionalAllShortestDiagnosticDocument(telemetry.Diagnostic.InvocationID)
	require.NoError(t, applyBidirectionalAllShortestTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "9123"))
	metrics := PostgresPlanMetrics{
		HydrationRows:  48,
		HydrationLoops: 12,
		PlanNodes: []PostgresPlanNodeMetric{{
			NodeType:      "Index Scan",
			RelationName:  "node",
			Alias:         "hydrated_nodes",
			ActualRows:    4,
			ActualLoops:   12,
			ActualTotalMS: .25,
		}},
	}
	enrichBidirectionalHydrationTelemetry(telemetry, document.Counters.OutputPaths, document.Counters.OutputEdgeCells, []string{`["p1"]`, `["p2"]`}, metrics)
	require.NoError(t, telemetry.Validate())
	require.Equal(t, TraversalTelemetryCounterStatusComplete, telemetry.Diagnostic.CounterStatus)
	require.Equal(t, int64(12), *telemetry.Diagnostic.Counters.Hydration.PathCount)
	require.Equal(t, int64(36), *telemetry.Diagnostic.Counters.Hydration.EdgeLookups)
	require.Equal(t, int64(48), *telemetry.Diagnostic.Counters.Hydration.NodeLookups)
}

// TestPostgresTraversalTelemetryRebindsASPExactFallbackAndRejectsMissingCounters verifies postgres traversal telemetry rebinds asp exact fallback and rejects missing counters behavior.
func TestPostgresTraversalTelemetryRebindsASPExactFallbackAndRejectsMissingCounters(t *testing.T) {
	telemetry := bidirectionalASPCaseTelemetry(t)
	document := validBidirectionalAllShortestDiagnosticDocument(telemetry.Diagnostic.InvocationID)
	document.RuntimeBranch = "exact_a1_fallback"
	document.Overflowed = traversalTelemetryPointer(true)
	document.FallbackExecuted = traversalTelemetryPointer(true)
	document.Calls[0].RuntimeBranch = "exact_a1_fallback"
	document.Calls[0].Overflowed = traversalTelemetryPointer(true)
	document.Calls[0].FallbackExecuted = traversalTelemetryPointer(true)

	require.NoError(t, applyBidirectionalAllShortestTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "9123"))
	require.NoError(t, telemetry.Validate())
	require.Equal(t, "ASP-A1-DAG", telemetry.Summary.RuntimeIdentity)
	require.Equal(t, "ASP-A1-DAG", telemetry.Summary.FallbackIdentity)
	require.Contains(t, telemetry.Diagnostic.IncompleteReasons, "nested exact ASP-A1 fallback traversal work counters are unavailable")

	telemetry = bidirectionalASPCaseTelemetry(t)
	document = validBidirectionalAllShortestDiagnosticDocument(telemetry.Diagnostic.InvocationID)
	document.Counters.OutputBytes = nil
	err := applyBidirectionalAllShortestTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "9123")
	require.ErrorContains(t, err, "output_bytes")
}

// TestPostgresTraversalTelemetryWitnessRequiresSeparateHydrationEvidence verifies postgres traversal telemetry witness requires separate hydration evidence behavior.
func TestPostgresTraversalTelemetryWitnessRequiresSeparateHydrationEvidence(t *testing.T) {
	telemetry := bidirectionalCaseTelemetry(t, TraversalTelemetryLevelDiagnostic)
	telemetry.Summary.RequestedIdentity = "SP-B2-C-MIN-LEVEL-WE+MAT-M0"
	telemetry.Summary.PlannedIdentities = []string{"SP-B2-C-MIN-LEVEL-WE+MAT-M0", "SP-S4-C-WE+MAT-M0"}
	telemetry.Summary.EmittedIdentity = "SP-B2-C-MIN-LEVEL-WE+MAT-M0"
	document := validBidirectionalDiagnosticDocument(telemetry.Diagnostic.InvocationID)

	require.NoError(t, applyBidirectionalTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "9123"))
	require.NoError(t, telemetry.Validate())
	require.Contains(t, telemetry.Diagnostic.RequiredFamilies, TraversalTelemetryFamilyHydration)
	require.Equal(t, TraversalTelemetryCounterStatusHiddenUnavailable, telemetry.Diagnostic.CounterStatus)
	require.Contains(t, telemetry.Diagnostic.IncompleteReasons, "complete invocation-local path hydration counters are unavailable")
}

// TestPostgresTraversalTelemetryLeavesNonBidirectionalHiddenFunctionsUnavailable verifies postgres traversal telemetry leaves non bidirectional hidden functions unavailable behavior.
func TestPostgresTraversalTelemetryLeavesNonBidirectionalHiddenFunctionsUnavailable(t *testing.T) {
	metrics := PostgresPlanMetrics{
		PlanNodes: []PostgresPlanNodeMetric{{
			NodeType:     "Function Scan",
			FunctionName: "all_shortest_paths_dag",
			ActualLoops:  1,
		}},
		Provenance: map[string]string{},
	}
	reference := PostgresReferenceResult{
		Architecture:     "ASP-A1-DAG",
		ImplementationID: "typed_predecessor_dag_v1",
		PostgresMetrics:  &metrics,
	}

	telemetry, err := buildPostgresReferenceTraversalTelemetry(reference, nil, "9123", TraversalTelemetryLevelDiagnostic)
	require.NoError(t, err)
	require.NoError(t, telemetry.Validate())
	require.Equal(t, TraversalTelemetryCounterStatusHiddenUnavailable, telemetry.Diagnostic.CounterStatus)
	require.Nil(t, telemetry.Diagnostic.Counters.AllShortestPaths)
	require.Contains(t, telemetry.Diagnostic.IncompleteReasons[0], "Function Scan")
}

// TestPostgresTraversalTelemetryUsesPlanReplayForSQLVisibleOrientation verifies postgres traversal telemetry uses plan replay for sql visible orientation behavior.
func TestPostgresTraversalTelemetryUsesPlanReplayForSQLVisibleOrientation(t *testing.T) {
	outcome := translate.TargetLoweringOutcome{
		Family:            "fixed_suffix_expansion",
		Candidate:         "EXPANSION-SUFFIX-SEEDED-REVERSE",
		Selected:          "EXPANSION-STEPWISE-FORWARD",
		Applied:           "EXPANSION-STEPWISE-FORWARD",
		Fallback:          "EXPANSION-STEPWISE-FORWARD",
		PlannedCandidates: []string{"EXPANSION-SUFFIX-SEEDED-REVERSE", "EXPANSION-STEPWISE-FORWARD"},
		EmittedCandidates: []string{"EXPANSION-SUFFIX-SEEDED-REVERSE", "EXPANSION-STEPWISE-FORWARD"},
		EmittedPolicy:     "orientation-probe-v1",
		SelectorVersion:   "orientation-probe-v1",
		ExecutionBoundary: "guarded_dual_arm",
		StateLimit:        4096,
	}
	metrics := PostgresPlanMetrics{
		PlanNodes: []PostgresPlanNodeMetric{{
			NodeType:    "Result",
			SubplanName: "CTE s5_orientation_executed_candidate",
			ActualRows:  1,
			ActualLoops: 1,
		}},
		Provenance: map[string]string{},
	}

	telemetry, err := buildPostgresCaseTraversalTelemetry(
		translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}},
		metrics,
		"9123",
		TraversalTelemetryLevelDiagnostic,
	)
	require.NoError(t, err)
	require.NoError(t, telemetry.Validate())
	require.Equal(t, TraversalTelemetryFamilyOrientation, telemetry.Diagnostic.RequiredFamilies[0])
	require.Equal(t, TraversalTelemetryCounterStatusPlanPartial, telemetry.Diagnostic.CounterStatus)
	require.Equal(t, "EXPANSION-SUFFIX-SEEDED-REVERSE", telemetry.Summary.RuntimeIdentity)
	require.Equal(t, telemetry.Summary.RuntimeIdentity, telemetry.Summary.AppliedIdentity)
	require.Equal(t, "guarded_dual_arm", telemetry.Summary.ExecutionBoundary)
	require.Equal(t, int64(1), telemetry.Diagnostic.PlanReplay.Counters["orientation_executed_candidate_rows"])
}

// TestPostgresTraversalTelemetryKeepsEndpointGuardInOrientationFamily verifies postgres traversal telemetry keeps endpoint guard in orientation family behavior.
func TestPostgresTraversalTelemetryKeepsEndpointGuardInOrientationFamily(t *testing.T) {
	outcome := translate.TargetLoweringOutcome{
		Family:            "fixed_prefix_terminal_expansion",
		Candidate:         string(optimize.ExpansionSearchEndpointSeededReverse),
		Selected:          string(optimize.ExpansionSearchEndpointSeededReverse),
		Applied:           string(optimize.ExpansionSearchEndpointSeededReverse),
		Fallback:          string(optimize.ExpansionSearchStepwiseForward),
		PlannedCandidates: []string{string(optimize.ExpansionSearchStepwiseForward), string(optimize.ExpansionSearchEndpointSeededReverse)},
		EmittedCandidates: []string{string(optimize.ExpansionSearchStepwiseForward), string(optimize.ExpansionSearchEndpointSeededReverse)},
		EmittedPolicy:     string(optimize.ExpansionSearchPolicyEndpointGuardV1),
		ExecutionBoundary: "guarded_dual_arm",
	}
	metrics := PostgresPlanMetrics{
		Provenance: map[string]string{},
		PlanNodes: []PostgresPlanNodeMetric{
			{
				NodeType:    "Result",
				SubplanName: "CTE s5_orientation_executed_candidate",
				ActualRows:  1,
				ActualLoops: 1,
			},
			{
				NodeType:    "Result",
				SubplanName: "CTE s5_orientation_executed_incumbent",
				ActualRows:  0,
				ActualLoops: 1,
			},
		},
	}

	telemetry, err := buildPostgresCaseTraversalTelemetry(
		translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelDiagnostic,
	)
	require.NoError(t, err)
	require.NoError(t, telemetry.Validate())
	require.Equal(t, []TraversalTelemetryFamily{TraversalTelemetryFamilyOrientation}, telemetry.Diagnostic.RequiredFamilies)
	require.Equal(t, string(optimize.ExpansionSearchEndpointSeededReverse), telemetry.Summary.RuntimeIdentity)
}

// TestPostgresTraversalTelemetryCompletesGuardedInlineASPCounters verifies postgres traversal telemetry completes guarded inline asp counters behavior.
func TestPostgresTraversalTelemetryCompletesGuardedInlineASPCounters(t *testing.T) {
	outcome := translate.TargetLoweringOutcome{
		Family:            "ASP",
		Candidate:         "ASP-I1-U-DAG+MAT-M0",
		Selected:          "ASP-I1-U-DAG+MAT-M0",
		Applied:           "ASP-I1-U-DAG+MAT-M0",
		Fallback:          "ASP-A1-DAG",
		PlannedCandidates: []string{"ASP-A1-DAG", "ASP-I1-U-DAG+MAT-M0"},
		EmittedCandidates: []string{"ASP-I1-U-DAG+MAT-M0", "ASP-A1-DAG"},
		EmittedPolicy:     "asp-i1-guarded-v1",
		SelectionMode:     "production_canary",
		SelectorVersion:   "asp-i1-canary-v1",
		ExecutionBoundary: "guarded_dual_arm",
		ObservationMode:   "all_paths",
		StateLimit:        10,
		PredecessorLimit:  20,
		EnumerationLimit:  30,
		OutputBytesLimit:  1000,
	}
	metrics := PostgresPlanMetrics{
		Provenance:     map[string]string{},
		HydrationRows:  4,
		HydrationLoops: 2,
		PlanNodes: []PostgresPlanNodeMetric{
			inlinePredecessorPlanNode("asp_i1_distance_bounded", 3, 1),
			inlinePredecessorPlanNode("asp_i1_predecessor_bounded", 2, 1),
			inlinePredecessorPlanNode("asp_i1_paths_bounded", 4, 1),
			inlinePredecessorPlanNode("asp_i1_shortest", 2, 1),
			inlinePredecessorPlanNode("asp_i1_candidate_marker", 1, 1),
			inlinePredecessorPlanNode("asp_i1_fallback_marker", 0, 1),
			inlinePredecessorPlanNode("asp_i1_candidate_rows", 2, 1),
			inlinePredecessorPlanNode("asp_i1_fallback_rows", 0, 1),
			inlinePredecessorMarkerGateNode("candidate", 1, 1),
			inlinePredecessorMarkerGateNode("fallback", 0, 1),
			inlinePredecessorExecutorNode("candidate", 1),
			inlinePredecessorExecutorNode("fallback", 0),
		},
	}
	telemetry, err := buildPostgresCaseTraversalTelemetry(
		translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelDiagnostic,
	)
	require.NoError(t, err)
	enrichInlineASPTraversalTelemetry(telemetry, metrics, 2, []string{`["p1"]`, `["p2"]`})
	require.NoError(t, telemetry.Validate())
	require.Equal(t, "ASP-I1-U-DAG+MAT-M0", telemetry.Summary.RuntimeIdentity)
	require.Equal(t, "inline_predecessor_dag", telemetry.Summary.RuntimeBranch)
	require.False(t, *telemetry.Summary.FallbackExecuted)
	require.Equal(t, TraversalTelemetryCounterStatusComplete, telemetry.Diagnostic.CounterStatus)
	require.Equal(t, int64(3), *telemetry.Diagnostic.Counters.InlineASP.DistanceRows)
	require.Equal(t, int64(2), *telemetry.Diagnostic.Counters.InlineASP.PredecessorRows)
	require.Equal(t, int64(4), *telemetry.Diagnostic.Counters.InlineASP.EnumerationRows)
	require.Equal(t, int64(1), *telemetry.Diagnostic.Counters.InlineASP.CandidateMarkerRows)
	require.Equal(t, int64(0), *telemetry.Diagnostic.Counters.InlineASP.FallbackMarkerRows)
	require.Equal(t, int64(1), *telemetry.Diagnostic.Counters.InlineASP.CandidateExecutorLoops)
	require.Equal(t, int64(0), *telemetry.Diagnostic.Counters.InlineASP.FallbackExecutorLoops)
}

// TestPostgresTraversalTelemetryCompletesGuardedInlineCanonicalSPCounters verifies postgres traversal telemetry completes guarded inline canonical sp counters behavior.
func TestPostgresTraversalTelemetryCompletesGuardedInlineCanonicalSPCounters(t *testing.T) {
	outcome := translate.TargetLoweringOutcome{
		Family:    "SP",
		Candidate: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		Selected:  string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		Applied:   string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		Fallback:  string(optimize.ShortestPathExecutorS4CanonicalWitness),
		PlannedCandidates: []string{
			string(optimize.ShortestPathExecutorS4CanonicalWitness),
			string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		},
		EmittedCandidates: []string{
			string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
			string(optimize.ShortestPathExecutorS4CanonicalWitness),
		},
		EmittedPolicy:     optimize.ShortestPathPolicyI1CanonicalGuardedV1,
		SelectionMode:     "production_canary",
		SelectorVersion:   "sp-i1-canary-v1",
		ExecutionBoundary: "guarded_dual_arm",
		ObservationMode:   "one_path",
		StateLimit:        10,
		PredecessorLimit:  20,
		EnumerationLimit:  30,
		OutputBytesLimit:  1000,
	}

	tests := []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// candidateMarker retains the candidate marker while anonymous record is assembled or evaluated.
		candidateMarker int64
		// fallbackMarker retains the fallback marker while anonymous record is assembled or evaluated.
		fallbackMarker int64
		// outputRows records the number of output rows.
		outputRows int64
		// distanceRows records the number of distance rows.
		distanceRows int64
		// expectedIdentity identifies the expected identity.
		expectedIdentity string
		// expectedBranch retains the expected branch while anonymous record is assembled or evaluated.
		expectedBranch string
		// expectedFallback indicates whether expected fallback applies.
		expectedFallback bool
	}{
		{
			name:             "candidate witness",
			candidateMarker:  1,
			outputRows:       1,
			distanceRows:     3,
			expectedIdentity: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
			expectedBranch:   "inline_canonical_witness",
		},
		{
			name:             "candidate no path",
			candidateMarker:  1,
			distanceRows:     3,
			expectedIdentity: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
			expectedBranch:   "inline_canonical_no_path",
		},
		{
			name:             "exact S4 fallback",
			fallbackMarker:   1,
			outputRows:       1,
			distanceRows:     11,
			expectedIdentity: string(optimize.ShortestPathExecutorS4CanonicalWitness),
			expectedBranch:   "exact_s4_fallback",
			expectedFallback: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metrics := PostgresPlanMetrics{
				Provenance:     map[string]string{},
				HydrationRows:  test.outputRows,
				HydrationLoops: test.outputRows,
				PlanNodes: []PostgresPlanNodeMetric{
					inlinePredecessorPlanNode("asp_i1_distance_bounded", test.distanceRows, 1),
					inlinePredecessorPlanNode("asp_i1_predecessor_bounded", 2, 1),
					inlinePredecessorPlanNode("asp_i1_paths_bounded", 4, 1),
					inlinePredecessorPlanNode("asp_i1_shortest", test.outputRows, 1),
					inlinePredecessorPlanNode("asp_i1_candidate_marker", test.candidateMarker, 1),
					inlinePredecessorPlanNode("asp_i1_fallback_marker", test.fallbackMarker, 1),
					inlinePredecessorPlanNode("asp_i1_candidate_rows", test.candidateMarker*test.outputRows, 1),
					inlinePredecessorPlanNode("asp_i1_fallback_rows", test.fallbackMarker*test.outputRows, 1),
					inlinePredecessorMarkerGateNode("candidate", test.candidateMarker, 1),
					inlinePredecessorMarkerGateNode("fallback", test.fallbackMarker, 1),
					inlinePredecessorExecutorNode("candidate", test.candidateMarker),
					inlinePredecessorExecutorNode("fallback", test.fallbackMarker),
				},
			}
			telemetry, err := buildPostgresCaseTraversalTelemetry(
				translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelDiagnostic,
			)
			require.NoError(t, err)
			enrichInlinePredecessorTraversalTelemetry(telemetry, metrics, test.outputRows, []string{`["p1"]`})
			require.NoError(t, telemetry.Validate())
			require.Equal(t, test.expectedIdentity, telemetry.Summary.RuntimeIdentity)
			require.Equal(t, test.expectedBranch, telemetry.Summary.RuntimeBranch)
			require.Equal(t, test.expectedFallback, *telemetry.Summary.FallbackExecuted)
			require.Equal(t, TraversalTelemetryCounterStatusComplete, telemetry.Diagnostic.CounterStatus)
			require.NotNil(t, telemetry.Diagnostic.Counters.InlineShortestPath)
			require.Nil(t, telemetry.Diagnostic.Counters.InlineASP)
			require.Equal(t, test.candidateMarker, *telemetry.Diagnostic.Counters.InlineShortestPath.CandidateMarkerRows)
			require.Equal(t, test.fallbackMarker, *telemetry.Diagnostic.Counters.InlineShortestPath.FallbackMarkerRows)
			require.Equal(t, test.candidateMarker, *telemetry.Diagnostic.Counters.InlineShortestPath.CandidateExecutorLoops)
			require.Equal(t, test.fallbackMarker, *telemetry.Diagnostic.Counters.InlineShortestPath.FallbackExecutorLoops)
		})
	}
}

func TestPostgresTraversalTelemetryCompletesGuardedInlineDistanceCounters(t *testing.T) {
	outcome := translate.TargetLoweringOutcome{
		Family: "SP", Candidate: string(optimize.ShortestPathExecutorI2GuardedDistance), Selected: string(optimize.ShortestPathExecutorI2GuardedDistance),
		Applied: string(optimize.ShortestPathExecutorI2GuardedDistance), Fallback: string(optimize.ShortestPathExecutorS4CanonicalDistance),
		EmittedPolicy: optimize.ShortestPathPolicyI2DistanceGuardedV1, SelectionMode: "production_canary",
		SelectorVersion: optimize.ShortestPathSelectorStaticV8HiddenFanIn, ExecutionBoundary: "guarded_dual_arm", ObservationMode: "distance",
		StateLimit: 10, FrontierLimit: 10,
	}
	ids := map[string]int64{"sp_i2_distance_bounded": 1, "sp_i2_target": 2, "sp_i2_candidate_marker": 3, "sp_i2_fallback_marker": 4, "sp_i2_candidate_rows": 5, "sp_i2_fallback_rows": 6}
	node := func(name string, rows int64) PostgresPlanNodeMetric {
		return PostgresPlanNodeMetric{PlanNodeID: ids[name], NodeType: "Result", SubplanName: "CTE " + name, ActualRows: rows, ActualLoops: 1}
	}
	gate := func(branch string, markerRows int64) []PostgresPlanNodeMetric {
		body := ids["sp_i2_"+branch+"_rows"]
		return []PostgresPlanNodeMetric{
			{PlanNodeID: body + 100, ParentPlanNodeID: body, ParentRelationship: "Outer", NodeType: "CTE Scan", CTEName: "sp_i2_" + branch + "_marker", ActualRows: markerRows, ActualLoops: 1},
			{PlanNodeID: body + 200, ParentPlanNodeID: body, ParentRelationship: "Inner", NodeType: "Result", ActualLoops: markerRows},
		}
	}
	metrics := PostgresPlanMetrics{Provenance: map[string]string{}, PlanNodes: []PostgresPlanNodeMetric{
		node("sp_i2_distance_bounded", 3), node("sp_i2_target", 1), node("sp_i2_candidate_marker", 1), node("sp_i2_fallback_marker", 0),
		node("sp_i2_candidate_rows", 1), node("sp_i2_fallback_rows", 0),
	}}
	metrics.PlanNodes = append(metrics.PlanNodes, gate("candidate", 1)...)
	metrics.PlanNodes = append(metrics.PlanNodes, gate("fallback", 0)...)
	telemetry, err := buildPostgresCaseTraversalTelemetry(translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelDiagnostic)
	require.NoError(t, err)
	enrichInlineDistanceTraversalTelemetry(telemetry, 1)
	require.NoError(t, telemetry.Validate())
	require.Equal(t, string(optimize.ShortestPathExecutorI2GuardedDistance), telemetry.Summary.RuntimeIdentity)
	require.Equal(t, "inline_canonical_distance", telemetry.Summary.RuntimeBranch)
	require.NotNil(t, telemetry.Diagnostic.Counters.InlineShortestDistance)
	require.Equal(t, int64(3), *telemetry.Diagnostic.Counters.InlineShortestDistance.StateRows)
	require.Equal(t, int64(1), *telemetry.Diagnostic.Counters.InlineShortestDistance.CandidateExecutorLoops)
	require.Equal(t, int64(0), *telemetry.Diagnostic.Counters.InlineShortestDistance.FallbackExecutorLoops)
	require.Equal(t, int64(1), telemetry.Diagnostic.PlanReplay.Counters["sp_i2_target_rows"])
	require.Equal(t, int64(1), telemetry.Diagnostic.PlanReplay.Counters["sp_i2_output_rows"])

	mismatched, err := buildPostgresCaseTraversalTelemetry(translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelDiagnostic)
	require.NoError(t, err)
	enrichInlineDistanceTraversalTelemetry(mismatched, 2)
	require.Equal(t, TraversalTelemetryCounterStatusHiddenUnavailable, mismatched.Diagnostic.CounterStatus)
	require.Contains(t, mismatched.Diagnostic.IncompleteReasons, "inline distance plan output does not match the exact public observation")
	require.Nil(t, mismatched.Diagnostic.Counters.InlineShortestDistance)
}

// TestResourceGateRequiresSingularInlineDistanceBranchAndInactiveArm verifies
// SP-I2 qualification binds complementary markers, branch rows, executor loops,
// typed counters, and the runtime receipt for both possible guarded arms.
func TestResourceGateRequiresSingularInlineDistanceBranchAndInactiveArm(t *testing.T) {
	newTelemetry := func(fallback bool) *TraversalExecutionTelemetry {
		const limit int64 = 3
		candidateMarker, fallbackMarker := int64(1), int64(0)
		candidateRows, fallbackRows := int64(1), int64(0)
		candidateLoops, fallbackLoops := int64(1), int64(0)
		stateRows := limit
		runtimeIdentity := string(optimize.ShortestPathExecutorI2GuardedDistance)
		runtimeBranch := "inline_canonical_distance"
		if fallback {
			candidateMarker, fallbackMarker = 0, 1
			candidateRows, fallbackRows = 0, 1
			candidateLoops, fallbackLoops = 0, 1
			stateRows = limit + 1
			runtimeIdentity = string(optimize.ShortestPathExecutorS4CanonicalDistance)
			runtimeBranch = "exact_s4_distance_fallback"
		}
		plan := map[string]int64{
			"sp_i2_distance_rows": stateRows, "sp_i2_target_rows": candidateRows, "sp_i2_output_rows": candidateRows + fallbackRows,
			"sp_i2_candidate_marker_rows": candidateMarker, "sp_i2_fallback_marker_rows": fallbackMarker,
			"sp_i2_candidate_branch_rows": candidateRows, "sp_i2_fallback_branch_rows": fallbackRows,
			"sp_i2_candidate_executor_loops": candidateLoops, "sp_i2_fallback_executor_loops": fallbackLoops,
		}
		return &TraversalExecutionTelemetry{
			Summary: TraversalExecutionSummary{
				EmittedIdentity: optimize.ShortestPathPolicyI2DistanceGuardedV1, RuntimeIdentity: runtimeIdentity, RuntimeBranch: runtimeBranch,
				Caps:                    map[string]int64{"state_rows": limit, "frontier_rows": limit},
				RuntimeOutcomeAvailable: telemetryBool(true), FallbackExecuted: telemetryBool(fallback), Overflow: telemetryBool(fallback),
			},
			Diagnostic: &TraversalExecutionDiagnostic{
				RequiredFamilies: []TraversalTelemetryFamily{TraversalTelemetryFamilySP},
				Counters: TraversalDiagnosticCounters{InlineShortestDistance: &InlineDistanceTraversalCounters{
					StateRows: telemetryInt64(stateRows), FrontierRows: telemetryInt64(stateRows), OutputRows: telemetryInt64(candidateRows + fallbackRows),
					CandidateMarkerRows: telemetryInt64(candidateMarker), FallbackMarkerRows: telemetryInt64(fallbackMarker),
					CandidateBranchRows: telemetryInt64(candidateRows), FallbackBranchRows: telemetryInt64(fallbackRows),
					CandidateExecutorLoops: telemetryInt64(candidateLoops), FallbackExecutorLoops: telemetryInt64(fallbackLoops),
				}},
				PlanReplay: &TraversalPlanReplayEvidence{Counters: plan},
			},
		}
	}

	t.Run("candidate exact cap boundary", func(t *testing.T) {
		gateCase := &ResourceGateCase{}
		appendInlineDistanceAttributionReasons(gateCase, newTelemetry(false))
		require.Empty(t, gateCase.Reasons)
	})
	t.Run("fallback exact cap plus one sentinel", func(t *testing.T) {
		gateCase := &ResourceGateCase{}
		appendInlineDistanceAttributionReasons(gateCase, newTelemetry(true))
		require.Empty(t, gateCase.Reasons)
	})

	tests := map[string]struct {
		mutate func(*TraversalExecutionTelemetry)
		reason string
	}{
		"inactive fallback executor": {func(telemetry *TraversalExecutionTelemetry) {
			telemetry.Diagnostic.PlanReplay.Counters["sp_i2_fallback_executor_loops"] = 1
		}, "candidate selection did not suppress the fallback executor"},
		"dual markers": {func(telemetry *TraversalExecutionTelemetry) {
			telemetry.Diagnostic.PlanReplay.Counters["sp_i2_fallback_marker_rows"] = 1
		}, "must attribute exactly one candidate or fallback marker"},
		"branch output mismatch": {func(telemetry *TraversalExecutionTelemetry) {
			telemetry.Diagnostic.PlanReplay.Counters["sp_i2_output_rows"] = 0
		}, "output does not equal its complementary branch rows"},
		"typed counter drift": {func(telemetry *TraversalExecutionTelemetry) {
			telemetry.Diagnostic.Counters.InlineShortestDistance.CandidateExecutorLoops = telemetryInt64(0)
		}, "typed counter does not match plan counter sp_i2_candidate_executor_loops"},
		"runtime receipt drift": {func(telemetry *TraversalExecutionTelemetry) {
			telemetry.Summary.RuntimeBranch = "exact_s4_distance_fallback"
		}, "candidate marker contradicts the runtime receipt"},
		"candidate cap plus one": {func(telemetry *TraversalExecutionTelemetry) {
			value := telemetry.Summary.Caps["state_rows"] + 1
			telemetry.Diagnostic.PlanReplay.Counters["sp_i2_distance_rows"] = value
			telemetry.Diagnostic.Counters.InlineShortestDistance.StateRows = telemetryInt64(value)
			telemetry.Diagnostic.Counters.InlineShortestDistance.FrontierRows = telemetryInt64(value)
		}, "candidate selection exceeds its state or conservative frontier cap"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			telemetry := newTelemetry(false)
			test.mutate(telemetry)
			gateCase := &ResourceGateCase{}
			appendInlineDistanceAttributionReasons(gateCase, telemetry)
			require.Contains(t, strings.Join(gateCase.Reasons, "\n"), test.reason)
		})
	}

	for name, stateRows := range map[string]int64{
		"fallback without sentinel":    3,
		"fallback beyond cap plus one": 5,
	} {
		t.Run(name, func(t *testing.T) {
			telemetry := newTelemetry(true)
			telemetry.Diagnostic.PlanReplay.Counters["sp_i2_distance_rows"] = stateRows
			telemetry.Diagnostic.Counters.InlineShortestDistance.StateRows = telemetryInt64(stateRows)
			telemetry.Diagnostic.Counters.InlineShortestDistance.FrontierRows = telemetryInt64(stateRows)
			gateCase := &ResourceGateCase{}
			appendInlineDistanceAttributionReasons(gateCase, telemetry)
			require.Contains(t, gateCase.Reasons, "inline SP distance fallback selection lacks an exact state or conservative frontier cap+1 sentinel")
		})
	}

	telemetry := newTelemetry(false)
	record := CaseResult{
		RowCount: 1, TraversalTelemetry: telemetry,
		Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
			Family: "SP", Applied: string(optimize.ShortestPathExecutorI2GuardedDistance), EmittedPolicy: optimize.ShortestPathPolicyI2DistanceGuardedV1,
		}}},
	}
	contract, found := guardedInlineResourceContractForArchitecture(string(optimize.ShortestPathExecutorI2GuardedDistance))
	require.True(t, found)
	gateCase := &ResourceGateCase{}
	appendGuardedInlineResourceBindingReasons(gateCase, record, contract)
	require.Empty(t, gateCase.Reasons)
	record.RowCount = 2
	appendGuardedInlineResourceBindingReasons(gateCase, record, contract)
	require.Contains(t, gateCase.Reasons, "inline SP distance typed output does not match the exact public observation")
	require.Contains(t, gateCase.Reasons, "inline SP distance plan output does not match the exact public observation")
}

// TestPostgresTraversalTelemetryRejectsEveryMissingInlinePredecessorCounter verifies postgres traversal telemetry rejects every missing inline predecessor counter behavior.
func TestPostgresTraversalTelemetryRejectsEveryMissingInlinePredecessorCounter(t *testing.T) {
	outcome := translate.TargetLoweringOutcome{
		Family:    "SP",
		Candidate: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		Selected:  string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		Applied:   string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		Fallback:  string(optimize.ShortestPathExecutorS4CanonicalWitness),
		PlannedCandidates: []string{
			string(optimize.ShortestPathExecutorS4CanonicalWitness),
			string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		},
		EmittedCandidates: []string{
			string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
			string(optimize.ShortestPathExecutorS4CanonicalWitness),
		},
		EmittedPolicy:    optimize.ShortestPathPolicyI1CanonicalGuardedV1,
		ObservationMode:  "one_path",
		StateLimit:       10,
		PredecessorLimit: 20,
		EnumerationLimit: 30,
		OutputBytesLimit: 1000,
	}
	fullPlan := []PostgresPlanNodeMetric{
		inlinePredecessorPlanNode("asp_i1_distance_bounded", 3, 1),
		inlinePredecessorPlanNode("asp_i1_predecessor_bounded", 2, 1),
		inlinePredecessorPlanNode("asp_i1_paths_bounded", 4, 1),
		inlinePredecessorPlanNode("asp_i1_shortest", 1, 1),
		inlinePredecessorPlanNode("asp_i1_candidate_marker", 1, 1),
		inlinePredecessorPlanNode("asp_i1_fallback_marker", 0, 1),
		inlinePredecessorPlanNode("asp_i1_candidate_rows", 1, 1),
		inlinePredecessorPlanNode("asp_i1_fallback_rows", 0, 1),
		inlinePredecessorMarkerGateNode("candidate", 1, 1),
		inlinePredecessorMarkerGateNode("fallback", 0, 1),
		inlinePredecessorExecutorNode("candidate", 1),
		inlinePredecessorExecutorNode("fallback", 0),
	}
	expectedCounter := map[string]string{
		"asp_i1_distance_bounded":    "asp_i1_distance_rows",
		"asp_i1_predecessor_bounded": "asp_i1_predecessor_rows",
		"asp_i1_paths_bounded":       "asp_i1_enumeration_rows",
		"asp_i1_shortest":            "asp_i1_output_rows",
		"asp_i1_candidate_marker":    "asp_i1_candidate_marker_rows",
		"asp_i1_fallback_marker":     "asp_i1_fallback_marker_rows",
		"asp_i1_candidate_rows":      "asp_i1_candidate_branch_rows",
		"asp_i1_fallback_rows":       "asp_i1_fallback_branch_rows",
		"test_candidate_executor":    "asp_i1_candidate_executor_loops",
		"test_fallback_executor":     "asp_i1_fallback_executor_loops",
	}

	for omitted, counter := range expectedCounter {
		t.Run(omitted, func(t *testing.T) {
			metrics := PostgresPlanMetrics{Provenance: map[string]string{}}
			for _, node := range fullPlan {
				if node.SubplanName != "CTE "+omitted && node.Alias != omitted {
					metrics.PlanNodes = append(metrics.PlanNodes, node)
				}
			}
			telemetry, err := buildPostgresCaseTraversalTelemetry(
				translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelDiagnostic,
			)
			require.NoError(t, err)
			enrichInlinePredecessorTraversalTelemetry(telemetry, metrics, 1, []string{`["p1"]`})
			require.NoError(t, telemetry.Validate())
			require.Equal(t, TraversalTelemetryCounterStatusHiddenUnavailable, telemetry.Diagnostic.CounterStatus)
			require.Nil(t, telemetry.Diagnostic.Counters.InlineShortestPath)
			require.Contains(t, telemetry.Diagnostic.IncompleteReasons[0], counter)
		})
	}
}

// TestPostgresTraversalPlanReplayUsesExactInlinePredecessorCTEBodies verifies postgres traversal plan replay uses exact inline predecessor cte bodies behavior.
func TestPostgresTraversalPlanReplayUsesExactInlinePredecessorCTEBodies(t *testing.T) {
	metrics := PostgresPlanMetrics{
		Provenance: map[string]string{},
		PlanNodes: []PostgresPlanNodeMetric{
			inlinePredecessorPlanNode("asp_i1_distance_bounded", 3, 1),
			{
				PlanNodeID:  500,
				NodeType:    "Limit",
				SubplanName: "CTE prefix_asp_i1_distance_bounded",
				ActualRows:  77,
				ActualLoops: 1,
			},
			{
				NodeType:    "CTE Scan",
				CTEName:     "asp_i1_distance_bounded",
				Alias:       "asp_i1_distance_bounded",
				ActualRows:  99,
				ActualLoops: 7,
			},
			inlinePredecessorPlanNode("asp_i1_candidate_rows", 0, 1),
			{
				NodeType:    "CTE Scan",
				CTEName:     "asp_i1_candidate_rows",
				Alias:       "asp_i1_candidate_rows",
				ActualRows:  10,
				ActualLoops: 5,
			},
			inlinePredecessorPlanNode("asp_i1_fallback_rows", 0, 1),
			{
				NodeType:    "CTE Scan",
				CTEName:     "asp_i1_fallback_rows",
				Alias:       "asp_i1_fallback_rows",
				ActualRows:  8,
				ActualLoops: 3,
			},
			inlinePredecessorPlanNode("asp_i1_candidate_marker", 1, 1),
			inlinePredecessorPlanNode("asp_i1_fallback_marker", 0, 1),
			inlinePredecessorMarkerGateNode("candidate", 1, 1),
			inlinePredecessorMarkerGateNode("fallback", 0, 1),
			inlinePredecessorExecutorNode("candidate", 1),
			inlinePredecessorExecutorNode("fallback", 0),
		},
	}

	replay := postgresTraversalPlanReplay(metrics)
	require.Equal(t, int64(3), replay.Counters["asp_i1_distance_rows"])
	require.Equal(t, int64(0), replay.Counters["asp_i1_candidate_branch_rows"])
	require.Equal(t, int64(0), replay.Counters["asp_i1_fallback_branch_rows"])
	require.Equal(t, int64(1), replay.Counters["asp_i1_candidate_executor_loops"])
	require.Equal(t, int64(0), replay.Counters["asp_i1_fallback_executor_loops"])
}

// TestPostgresTraversalPlanReplayRejectsAmbiguousInlineBranchShape verifies postgres traversal plan replay rejects ambiguous inline branch shape behavior.
func TestPostgresTraversalPlanReplayRejectsAmbiguousInlineBranchShape(t *testing.T) {
	t.Run("duplicate exact body", func(t *testing.T) {
		body := inlinePredecessorPlanNode("asp_i1_candidate_rows", 1, 1)
		duplicate := body
		duplicate.PlanNodeID = 99
		replay := postgresTraversalPlanReplay(PostgresPlanMetrics{
			Provenance: map[string]string{},
			PlanNodes: []PostgresPlanNodeMetric{
				body, duplicate, inlinePredecessorPlanNode("asp_i1_candidate_marker", 1, 1),
				inlinePredecessorMarkerGateNode("candidate", 1, 1),
				inlinePredecessorExecutorNode("candidate", 1),
			},
		})
		_, branchPresent := replay.Counters["asp_i1_candidate_branch_rows"]
		_, executorPresent := replay.Counters["asp_i1_candidate_executor_loops"]
		require.False(t, branchPresent)
		require.False(t, executorPresent)
	})

	t.Run("wrong direct outer marker", func(t *testing.T) {
		body := inlinePredecessorPlanNode("asp_i1_candidate_rows", 1, 1)
		wrongMarker := inlinePredecessorMarkerGateNode("candidate", 1, 1)
		wrongMarker.CTEName = "asp_i1_fallback_marker"
		replay := postgresTraversalPlanReplay(PostgresPlanMetrics{
			Provenance: map[string]string{},
			PlanNodes: []PostgresPlanNodeMetric{
				body, inlinePredecessorPlanNode("asp_i1_candidate_marker", 1, 1), wrongMarker, inlinePredecessorExecutorNode("candidate", 1),
			},
		})
		require.Equal(t, int64(1), replay.Counters["asp_i1_candidate_branch_rows"])
		_, executorPresent := replay.Counters["asp_i1_candidate_executor_loops"]
		require.False(t, executorPresent)
	})
}

// inlinePredecessorPlanNode prepares or inspects test evidence for inline predecessor plan node.
func inlinePredecessorPlanNode(name string, rows, loops int64) PostgresPlanNodeMetric {
	return PostgresPlanNodeMetric{
		PlanNodeID:  inlinePredecessorPlanNodeID(name),
		NodeType:    "Result",
		SubplanName: "CTE " + name,
		ActualRows:  rows,
		ActualLoops: loops,
	}
}

// inlinePredecessorExecutorNode prepares or inspects test evidence for inline predecessor executor node.
func inlinePredecessorExecutorNode(branch string, loops int64) PostgresPlanNodeMetric {
	bodyID := inlinePredecessorPlanNodeID("asp_i1_" + branch + "_rows")
	return PostgresPlanNodeMetric{
		PlanNodeID:         bodyID + 100,
		ParentPlanNodeID:   bodyID,
		ParentRelationship: "Inner",
		NodeType:           "Result",
		Alias:              "test_" + branch + "_executor",
		ActualLoops:        loops,
	}
}

// inlinePredecessorMarkerGateNode prepares or inspects test evidence for inline predecessor marker gate node.
func inlinePredecessorMarkerGateNode(branch string, rows, loops int64) PostgresPlanNodeMetric {
	bodyID := inlinePredecessorPlanNodeID("asp_i1_" + branch + "_rows")
	return PostgresPlanNodeMetric{
		PlanNodeID:         bodyID + 200,
		ParentPlanNodeID:   bodyID,
		ParentRelationship: "Outer",
		NodeType:           "CTE Scan",
		CTEName:            "asp_i1_" + branch + "_marker",
		Alias:              "test_" + branch + "_marker_gate",
		ActualRows:         rows,
		ActualLoops:        loops,
	}
}

// inlinePredecessorPlanNodeID prepares or inspects test evidence for inline predecessor plan node id.
func inlinePredecessorPlanNodeID(name string) int64 {
	ids := map[string]int64{
		"asp_i1_distance_bounded": 1, "asp_i1_predecessor_bounded": 2,
		"asp_i1_paths_bounded": 3, "asp_i1_shortest": 4,
		"asp_i1_candidate_marker": 5, "asp_i1_fallback_marker": 6,
		"asp_i1_candidate_rows": 7, "asp_i1_fallback_rows": 8,
	}
	return ids[name]
}

// TestPostgresTraversalTelemetryCompletesSuffixReverseGuardCounters verifies
// that the reverse-first guard uses its own counter family and reports both
// mutually exclusive runtime outcomes without orientation-only score fields.
func TestPostgresTraversalTelemetryCompletesSuffixReverseGuardCounters(t *testing.T) {
	tests := []struct {
		name             string
		candidateMarker  int64
		fallbackMarker   int64
		candidateLoops   int64
		fallbackLoops    int64
		suffixRows       int64
		stateRows        int64
		expectedIdentity string
		expectedBranch   string
		expectedFallback bool
	}{
		{name: "candidate", candidateMarker: 1, candidateLoops: 1, suffixRows: 2, stateRows: 9, expectedIdentity: string(optimize.ExpansionSearchSuffixSeededReverse), expectedBranch: "suffix_seeded_reverse"},
		{name: "suffix overflow fallback", fallbackMarker: 1, fallbackLoops: 1, suffixRows: 513, stateRows: 0, expectedIdentity: string(optimize.ExpansionSearchStepwiseForward), expectedBranch: "exact_forward_suffix_overflow", expectedFallback: true},
		{name: "state overflow fallback", fallbackMarker: 1, fallbackLoops: 1, suffixRows: 2, stateRows: 513, expectedIdentity: string(optimize.ExpansionSearchStepwiseForward), expectedBranch: "exact_forward_state_overflow", expectedFallback: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome := suffixGuardTestOutcome()
			metrics := suffixGuardTestMetrics(test.candidateMarker, test.fallbackMarker, test.candidateLoops, test.fallbackLoops, test.suffixRows, test.stateRows)
			telemetry, err := buildPostgresCaseTraversalTelemetry(
				translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelDiagnostic,
			)
			require.NoError(t, err)
			enrichSuffixGuardTraversalTelemetry(telemetry, metrics, 1, []string{`{"path":"p"}`})
			require.NoError(t, telemetry.Validate())
			require.Equal(t, test.expectedIdentity, telemetry.Summary.RuntimeIdentity)
			require.Equal(t, test.expectedBranch, telemetry.Summary.RuntimeBranch)
			require.Equal(t, test.expectedFallback, *telemetry.Summary.FallbackExecuted)
			require.Contains(t, telemetry.Diagnostic.RequiredFamilies, TraversalTelemetryFamilySuffixGuard)
			require.NotNil(t, telemetry.Diagnostic.Counters.SuffixGuard)
			require.Nil(t, telemetry.Diagnostic.Counters.Orientation)
			require.Equal(t, test.stateRows, *telemetry.Diagnostic.Counters.SuffixGuard.StateRows)
			require.Equal(t, test.candidateLoops, *telemetry.Diagnostic.Counters.SuffixGuard.CandidateExecutorLoops)
			require.Equal(t, test.fallbackLoops, *telemetry.Diagnostic.Counters.SuffixGuard.FallbackExecutorLoops)
		})
	}
}

// TestSuffixReverseGuardRuntimeOutcomeFailsClosedWithoutBothSentinels verifies
// that marker rows alone cannot invent an admitted branch when either cap+1
// relation is absent from the exact plan replay.
func TestSuffixReverseGuardRuntimeOutcomeFailsClosedWithoutBothSentinels(t *testing.T) {
	metrics := suffixGuardTestMetrics(1, 0, 1, 0, 2, 9)
	filtered := metrics.PlanNodes[:0]
	for _, node := range metrics.PlanNodes {
		if !strings.HasSuffix(strings.ToLower(node.SubplanName), "suffix_guard_states") {
			filtered = append(filtered, node)
		}
	}
	metrics.PlanNodes = filtered
	identity, branch, fallback, overflow := runtimeTraversalIdentity(
		suffixGuardTestOutcome(), metrics, string(optimize.ExpansionSearchSuffixSeededReverse), string(optimize.ExpansionSearchSuffixSeededReverse),
	)
	require.Empty(t, identity)
	require.Equal(t, "runtime_outcome_unavailable", branch)
	require.False(t, fallback)
	require.False(t, overflow)
}

// TestSuffixReverseGuardDiagnosticRejectsPlanOutputMismatch binds typed output
// telemetry to both the JSON plan and the exact public observation.
func TestSuffixReverseGuardDiagnosticRejectsPlanOutputMismatch(t *testing.T) {
	metrics := suffixGuardTestMetrics(1, 0, 1, 0, 2, 9)
	telemetry, err := buildPostgresCaseTraversalTelemetry(
		translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{suffixGuardTestOutcome()}}, metrics, "9123", TraversalTelemetryLevelDiagnostic,
	)
	require.NoError(t, err)
	enrichSuffixGuardTraversalTelemetry(telemetry, metrics, 2, []string{`{"path":"p1"}`, `{"path":"p2"}`})
	require.Equal(t, TraversalTelemetryCounterStatusHiddenUnavailable, telemetry.Diagnostic.CounterStatus)
	require.Contains(t, telemetry.Diagnostic.IncompleteReasons, "suffix-reverse guard plan output does not match the exact public observation")
}

func suffixGuardTestOutcome() translate.TargetLoweringOutcome {
	return translate.TargetLoweringOutcome{
		Family: "fixed_suffix_expansion", Candidate: string(optimize.ExpansionSearchSuffixSeededReverse),
		Selected: string(optimize.ExpansionSearchSuffixSeededReverse), Applied: string(optimize.ExpansionSearchSuffixSeededReverse),
		Fallback:          string(optimize.ExpansionSearchStepwiseForward),
		PlannedCandidates: []string{string(optimize.ExpansionSearchSuffixSeededReverse), string(optimize.ExpansionSearchStepwiseForward)},
		EmittedCandidates: []string{string(optimize.ExpansionSearchSuffixSeededReverse), string(optimize.ExpansionSearchStepwiseForward)},
		EmittedPolicy:     string(optimize.ExpansionSearchPolicySuffixReverseGuardV1),
		SelectorVersion:   optimize.ExpansionSearchSelectorFixedSuffixPathV1,
		ExecutionBoundary: optimize.ExpansionSearchExecutionBoundaryGuardedDualArm, ObservationMode: string(optimize.ExpansionSearchObservationFullPath),
		StateLimit: 512, ProbeCaps: &optimize.ExpansionSearchProbeCaps{ReverseSeedRowLimit: 512},
	}
}

// suffixGuardTestMetrics builds the exact marker-outer plan shape required by
// suffix guard qualification.
func suffixGuardTestMetrics(candidateMarker, fallbackMarker, candidateLoops, fallbackLoops, suffixRows, stateRows int64) PostgresPlanMetrics {
	stage := "s5_"
	planNode := func(id int64, suffix string, rows int64) PostgresPlanNodeMetric {
		return PostgresPlanNodeMetric{PlanNodeID: id, NodeType: "Result", SubplanName: "CTE " + stage + suffix, ActualRows: rows, ActualLoops: 1}
	}
	markerGate := func(id, bodyID int64, branch string, rows int64) PostgresPlanNodeMetric {
		return PostgresPlanNodeMetric{PlanNodeID: id, ParentPlanNodeID: bodyID, ParentRelationship: "Outer", NodeType: "CTE Scan", CTEName: stage + "suffix_guard_" + branch + "_marker", ActualRows: rows, ActualLoops: 1}
	}
	executor := func(id, bodyID int64, branch string, loops int64) PostgresPlanNodeMetric {
		return PostgresPlanNodeMetric{PlanNodeID: id, ParentPlanNodeID: bodyID, ParentRelationship: "Inner", NodeType: "Result", Alias: "suffix_guard_" + branch + "_executor", ActualLoops: loops}
	}
	return PostgresPlanMetrics{Provenance: map[string]string{}, RecursiveRows: stateRows, PlanNodes: []PostgresPlanNodeMetric{
		planNode(1, "suffix_guard_root_presence", 1), planNode(2, "suffix_guard_suffix_probe", suffixRows),
		planNode(3, "suffix_guard_boundaries", 1), planNode(4, "suffix_guard_states", stateRows),
		planNode(5, "suffix_guard_candidate_marker", candidateMarker), planNode(6, "suffix_guard_fallback_marker", fallbackMarker),
		planNode(7, "suffix_guard_candidate_body", candidateMarker), planNode(8, "suffix_guard_fallback_body", fallbackMarker),
		markerGate(70, 7, "candidate", candidateMarker), executor(71, 7, "candidate", candidateLoops),
		markerGate(80, 8, "fallback", fallbackMarker), executor(81, 8, "fallback", fallbackLoops),
	}}
}

// TestPostgresTraversalTelemetryPrefersShortestExecutorOverAnalysisOutcomes verifies postgres traversal telemetry prefers shortest executor over analysis outcomes behavior.
func TestPostgresTraversalTelemetryPrefersShortestExecutorOverAnalysisOutcomes(t *testing.T) {
	shortest := translate.TargetLoweringOutcome{
		TargetKind: "traversal",
		Family:     "SP",
		Applied:    "SP-B1-C-ALT-NODE-D",
	}
	outcome, found := singleTraversalOutcome([]translate.TargetLoweringOutcome{
		{
			TargetKind:      "endpoint_resolution",
			Family:          "endpoint_resolution",
			TraversalFamily: "SP",
			Applied:         "ENDPOINT-RESOLUTION-INCUMBENT",
		},
		{
			TargetKind: "traversal_predicate",
			Family:     "traversal_predicate",
			Applied:    "TRAVERSAL-PREDICATE-INCUMBENT",
		},
		{
			TargetKind: "traversal",
			Family:     "fixed_suffix_expansion",
			Applied:    "EXPANSION-STEPWISE-FORWARD",
		},
		shortest,
	})
	require.True(t, found)
	require.Equal(t, shortest, outcome)
}

// TestPostgresTraversalTelemetrySeparatesShadowChoiceFromExecutedIncumbent verifies postgres traversal telemetry separates shadow choice from executed incumbent behavior.
func TestPostgresTraversalTelemetrySeparatesShadowChoiceFromExecutedIncumbent(t *testing.T) {
	outcome := translate.TargetLoweringOutcome{
		Family:            "fixed_suffix_expansion",
		Candidate:         "EXPANSION-SUFFIX-SEEDED-REVERSE",
		Selected:          "EXPANSION-STEPWISE-FORWARD",
		Applied:           "EXPANSION-STEPWISE-FORWARD",
		Fallback:          "EXPANSION-STEPWISE-FORWARD",
		PlannedCandidates: []string{"EXPANSION-SUFFIX-SEEDED-REVERSE", "EXPANSION-STEPWISE-FORWARD"},
		EmittedCandidates: []string{"EXPANSION-STEPWISE-FORWARD"},
		EmittedPolicy:     "orientation-probe-v1",
		SelectionMode:     "shadow_tool",
		SelectorVersion:   "orientation-probe-v1",
		StateLimit:        4096,
		ProbeCaps: &optimize.ExpansionSearchProbeCaps{
			ReverseSeedRowLimit: 512,
		},
	}
	metrics := PostgresPlanMetrics{
		PlanNodes: []PostgresPlanNodeMetric{
			{
				NodeType:    "Result",
				SubplanName: "CTE s5_orientation_shadow_reverse",
				ActualRows:  1,
				ActualLoops: 1,
			},
			{
				NodeType:    "Result",
				SubplanName: "CTE s5_orientation_shadow_forward",
				ActualRows:  0,
				ActualLoops: 1,
			},
			{
				NodeType:    "Result",
				SubplanName: "CTE s5_orientation_executed_incumbent",
				ActualRows:  1,
				ActualLoops: 1,
			},
			{
				NodeType:    "Limit",
				SubplanName: "CTE s5_orientation_suffix_probe",
				ActualRows:  513,
				ActualLoops: 1,
			},
		},
		Provenance: map[string]string{},
	}

	telemetry, err := buildPostgresCaseTraversalTelemetry(
		translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}},
		metrics,
		"9123",
		TraversalTelemetryLevelSummary,
	)
	require.NoError(t, err)
	require.NoError(t, telemetry.Validate())
	require.Equal(t, "EXPANSION-STEPWISE-FORWARD", telemetry.Summary.RuntimeIdentity)
	require.Equal(t, "EXPANSION-STEPWISE-FORWARD", telemetry.Summary.AppliedIdentity)
	require.Equal(t, "EXPANSION-SUFFIX-SEEDED-REVERSE", telemetry.Summary.WouldSelectIdentity)
	require.Equal(t, "shadow_incumbent", telemetry.Summary.RuntimeBranch)
	require.False(t, *telemetry.Summary.FallbackExecuted)
	require.True(t, *telemetry.Summary.Overflow)
}

// TestPostgresTraversalTelemetryUsesExactGuardedOrientationReceiptBranches verifies postgres traversal telemetry uses exact guarded orientation receipt branches behavior.
func TestPostgresTraversalTelemetryUsesExactGuardedOrientationReceiptBranches(t *testing.T) {
	for _, testCase := range []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// candidateRows records the number of candidate rows.
		candidateRows int64
		// incumbentRows records the number of incumbent rows.
		incumbentRows int64
		// rootProbeRows records the number of root probe rows.
		rootProbeRows int64
		// runtimeIdentity identifies the runtime identity.
		runtimeIdentity string
		// runtimeBranch retains the runtime branch while anonymous record is assembled or evaluated.
		runtimeBranch string
		// fallbackExecuted indicates whether fallback executed applies.
		fallbackExecuted bool
		// overflow indicates whether overflow applies.
		overflow bool
	}{
		{
			name:            "reverse candidate",
			candidateRows:   1,
			runtimeIdentity: string(optimize.ExpansionSearchSuffixSeededReverse),
			runtimeBranch:   "suffix_seeded_reverse",
		},
		{
			name:            "forward selection",
			incumbentRows:   1,
			runtimeIdentity: string(optimize.ExpansionSearchStepwiseForward),
			runtimeBranch:   "exact_forward_incumbent",
		},
		{
			name:             "overflow fallback",
			incumbentRows:    1,
			rootProbeRows:    optimize.ExpansionSearchOrientationRootRowLimit + 1,
			runtimeIdentity:  string(optimize.ExpansionSearchStepwiseForward),
			runtimeBranch:    "exact_forward_incumbent",
			fallbackExecuted: true,
			overflow:         true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			outcome := translate.TargetLoweringOutcome{
				Family:            "fixed_suffix_expansion",
				Candidate:         string(optimize.ExpansionSearchSuffixSeededReverse),
				Selected:          string(optimize.ExpansionSearchStepwiseForward),
				Applied:           string(optimize.ExpansionSearchStepwiseForward),
				Fallback:          string(optimize.ExpansionSearchStepwiseForward),
				PlannedCandidates: []string{string(optimize.ExpansionSearchStepwiseForward), string(optimize.ExpansionSearchSuffixSeededReverse)},
				EmittedCandidates: []string{string(optimize.ExpansionSearchStepwiseForward), string(optimize.ExpansionSearchSuffixSeededReverse)},
				EmittedPolicy:     string(optimize.ExpansionSearchPolicyOrientationProbeV2),
				SelectorVersion:   string(optimize.ExpansionSearchPolicyOrientationProbeV2),
				ExecutionBoundary: optimize.ExpansionSearchExecutionBoundaryGuardedDualArm,
				ProbeCaps:         &optimize.ExpansionSearchProbeCaps{RootRowLimit: optimize.ExpansionSearchOrientationRootRowLimit},
			}
			metrics := PostgresPlanMetrics{
				Provenance: map[string]string{},
				PlanNodes: []PostgresPlanNodeMetric{
					{
						NodeType:    "Result",
						SubplanName: "CTE s5_orientation_executed_candidate",
						ActualRows:  testCase.candidateRows,
						ActualLoops: 1,
					},
					{
						NodeType:    "Result",
						SubplanName: "CTE s5_orientation_executed_incumbent",
						ActualRows:  testCase.incumbentRows,
						ActualLoops: 1,
					},
					{
						NodeType:    "Limit",
						SubplanName: "CTE s5_orientation_root_probe",
						ActualRows:  testCase.rootProbeRows,
						ActualLoops: 1,
					},
				},
			}

			telemetry, err := buildPostgresCaseTraversalTelemetry(
				translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelSummary,
			)
			require.NoError(t, err)
			require.Equal(t, testCase.runtimeIdentity, telemetry.Summary.RuntimeIdentity)
			require.Equal(t, testCase.runtimeBranch, telemetry.Summary.RuntimeBranch)
			require.Equal(t, testCase.fallbackExecuted, *telemetry.Summary.FallbackExecuted)
			require.Equal(t, testCase.overflow, *telemetry.Summary.Overflow)
			require.NoError(t, validateRuntimeReceiptEvents([]RuntimeReceiptEvent{{
				Ordinal:          1,
				RuntimeIdentity:  testCase.runtimeIdentity,
				RuntimeBranch:    testCase.runtimeBranch,
				FallbackExecuted: testCase.fallbackExecuted,
			}}, telemetry.Summary.RuntimeIdentity, telemetry.Summary.RuntimeBranch, telemetry.Summary.FallbackExecuted))
		})
	}
}

// TestPostgresTraversalTelemetryUsesV2DepthWeightedDiagnosticScore verifies postgres traversal telemetry uses v2 depth weighted diagnostic score behavior.
func TestPostgresTraversalTelemetryUsesV2DepthWeightedDiagnosticScore(t *testing.T) {
	maximumDepth := int64(16)
	outcome := translate.TargetLoweringOutcome{
		Family:          "fixed_suffix_expansion",
		Candidate:       string(optimize.ExpansionSearchSuffixSeededReverse),
		Selected:        string(optimize.ExpansionSearchStepwiseForward),
		Applied:         string(optimize.ExpansionSearchStepwiseForward),
		Fallback:        string(optimize.ExpansionSearchStepwiseForward),
		EmittedPolicy:   string(optimize.ExpansionSearchPolicyOrientationProbeV2),
		SelectorVersion: string(optimize.ExpansionSearchPolicyOrientationProbeV2),
		MaximumDepth:    &maximumDepth,
	}
	metrics := PostgresPlanMetrics{
		Provenance: map[string]string{},
		PlanNodes: []PostgresPlanNodeMetric{
			{
				NodeType:    "Limit",
				SubplanName: "CTE s5_orientation_root_probe",
				ActualRows:  2,
				ActualLoops: 1,
			},
			{
				NodeType:    "Limit",
				SubplanName: "CTE s5_orientation_forward_degree_probe",
				ActualRows:  8,
				ActualLoops: 1,
			},
			{
				NodeType:    "Result",
				SubplanName: "CTE s5_orientation_executed_incumbent",
				ActualRows:  1,
				ActualLoops: 1,
			},
		},
	}
	telemetry, err := buildPostgresCaseTraversalTelemetry(
		translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelDiagnostic,
	)
	require.NoError(t, err)
	enrichOrientationTraversalTelemetry(telemetry, metrics, 1, []string{`["path"]`}, maximumDepth)
	require.NoError(t, telemetry.Validate())
	require.Equal(t, float64(130), *telemetry.Diagnostic.Counters.Orientation.ForwardScore)
	require.Equal(t, maximumDepth, orientationPolicyMaximumDepth(translate.OptimizationSummary{
		TargetOutcomes: []translate.TargetLoweringOutcome{outcome},
	}, string(optimize.ExpansionSearchPolicyOrientationProbeV2)))
}

// TestPostgresTraversalTelemetryCompletesOrientationCountersFromNamedPlanNodes verifies postgres traversal telemetry completes orientation counters from named plan nodes behavior.
func TestPostgresTraversalTelemetryCompletesOrientationCountersFromNamedPlanNodes(t *testing.T) {
	outcome := translate.TargetLoweringOutcome{
		Family:            "fixed_suffix_expansion",
		Candidate:         "EXPANSION-SUFFIX-SEEDED-REVERSE",
		Selected:          "EXPANSION-STEPWISE-FORWARD",
		Applied:           "EXPANSION-STEPWISE-FORWARD",
		Fallback:          "EXPANSION-STEPWISE-FORWARD",
		PlannedCandidates: []string{"EXPANSION-SUFFIX-SEEDED-REVERSE", "EXPANSION-STEPWISE-FORWARD"},
		EmittedCandidates: []string{"EXPANSION-SUFFIX-SEEDED-REVERSE", "EXPANSION-STEPWISE-FORWARD"},
		EmittedPolicy:     "orientation-probe-v1",
		SelectionMode:     "production_canary",
		SelectorVersion:   "orientation-probe-v1",
		StateLimit:        4096,
	}
	metrics := PostgresPlanMetrics{
		Provenance: map[string]string{},
		PlanNodes: []PostgresPlanNodeMetric{
			{
				NodeType:      "Limit",
				SubplanName:   "CTE s5_orientation_root_probe",
				ActualRows:    2,
				ActualLoops:   1,
				ActualTotalMS: .01,
				Buffers:       Buffers{SharedHit: 1},
			},
			{
				NodeType:      "Limit",
				SubplanName:   "CTE s5_orientation_suffix_probe",
				ActualRows:    5,
				ActualLoops:   1,
				ActualTotalMS: .02,
			},
			{
				NodeType:      "Aggregate",
				SubplanName:   "CTE s5_orientation_boundaries",
				ActualRows:    3,
				ActualLoops:   1,
				ActualTotalMS: .01,
			},
			{
				PlanNodeID:    40,
				NodeType:      "Aggregate",
				SubplanName:   "CTE s5_orientation_forward_degree_probe",
				ActualRows:    1,
				ActualLoops:   1,
				ActualTotalMS: .01,
			},
			{
				PlanNodeID:         41,
				ParentPlanNodeID:   40,
				ParentRelationship: "Outer",
				NodeType:           "Limit",
				ActualRows:         8,
				ActualLoops:        1,
			},
			{
				PlanNodeID:    50,
				NodeType:      "Aggregate",
				SubplanName:   "CTE s5_orientation_reverse_degree_probe",
				ActualRows:    1,
				ActualLoops:   1,
				ActualTotalMS: .01,
			},
			{
				PlanNodeID:         51,
				ParentPlanNodeID:   50,
				ParentRelationship: "Outer",
				NodeType:           "Limit",
				ActualRows:         1,
				ActualLoops:        1,
			},
			{
				NodeType:    "Limit",
				SubplanName: "CTE s5_orientation_states",
				ActualRows:  4,
				ActualLoops: 1,
			},
			{
				NodeType:    "Result",
				SubplanName: "CTE s5_orientation_executed_candidate",
				ActualRows:  1,
				ActualLoops: 1,
			},
			{
				NodeType:    "Result",
				SubplanName: "CTE s5_orientation_executed_incumbent",
				ActualRows:  0,
				ActualLoops: 1,
			},
			{
				NodeType:    "Recursive Union",
				SubplanName: "CTE s5_orientation_reverse",
				ActualRows:  4,
				ActualLoops: 1,
			},
			{
				NodeType:    "Result",
				SubplanName: "CTE s5_orientation_decision",
				ActualRows:  1,
				ActualLoops: 1,
			},
			// Consumer scans are deliberately repeated and must not inflate the
			// single materialization's row, loop, or branch attribution.
			{
				NodeType:    "CTE Scan",
				CTEName:     "s5_orientation_root_probe",
				Alias:       "s5_orientation_root_probe",
				ActualRows:  2,
				ActualLoops: 3,
			},
			{
				NodeType:    "CTE Scan",
				CTEName:     "s5_orientation_reverse_degree_probe",
				Alias:       "s5_orientation_reverse_degree_probe",
				ActualRows:  1,
				ActualLoops: 7,
			},
		},
	}
	telemetry, err := buildPostgresCaseTraversalTelemetry(translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelDiagnostic)
	require.NoError(t, err)
	enrichOrientationTraversalTelemetry(telemetry, metrics, 1, []string{`["path"]`}, 0)
	require.NoError(t, telemetry.Validate())
	require.Equal(t, TraversalTelemetryCounterStatusComplete, telemetry.Diagnostic.CounterStatus)
	require.Equal(t, int64(5), *telemetry.Diagnostic.Counters.Orientation.ReverseSeeds)
	require.Equal(t, int64(2), *telemetry.Diagnostic.Counters.Orientation.DuplicateSeeds)
	require.Equal(t, int64(8), *telemetry.Diagnostic.Counters.Orientation.ForwardDegreeSamples)
	require.Equal(t, int64(1), *telemetry.Diagnostic.Counters.Orientation.ReverseDegreeSamples)
	require.Equal(t, "reverse", telemetry.Diagnostic.Counters.Orientation.SelectedSide)
	require.Equal(t, int64(1), telemetry.Diagnostic.PlanReplay.Counters["orientation_root_probe_loops"])
	require.Equal(t, int64(1), telemetry.Diagnostic.PlanReplay.Counters["orientation_candidate_branch_loops"])
	require.Equal(t, int64(0), telemetry.Diagnostic.PlanReplay.Counters["orientation_incumbent_branch_loops"])
}

// TestPostgresTraversalTelemetrySummaryAndDisabledModesDoNotAttachDiagnosticCounters verifies postgres traversal telemetry summary and disabled modes do not attach diagnostic counters behavior.
func TestPostgresTraversalTelemetrySummaryAndDisabledModesDoNotAttachDiagnosticCounters(t *testing.T) {
	summary := bidirectionalCaseTelemetry(t, TraversalTelemetryLevelSummary)
	require.NoError(t, summary.Validate())
	require.Nil(t, summary.Diagnostic)
	require.False(t, *summary.Summary.RuntimeOutcomeAvailable)
	require.Empty(t, summary.Summary.RuntimeIdentity)
	require.Empty(t, summary.Summary.AppliedIdentity)
	require.Nil(t, summary.Summary.FallbackExecuted)

	record := CaseResult{
		PostgresReferences: []PostgresReferenceResult{{
			traversalTelemetryParameters: map[string]any{"state_limit": int64(1)},
		}},
	}
	runner := postgresSQLRunner{traversalTelemetry: postgresTraversalTelemetryOff}
	require.NoError(t, runner.attachPostgresTraversalTelemetry(t.Context(), &record, nil))
	require.Nil(t, record.TraversalTelemetry)
	require.Nil(t, record.PostgresReferences[0].TraversalTelemetry)
	require.Nil(t, record.PostgresReferences[0].traversalTelemetryParameters)
}

// TestPostgresTraversalTelemetryAttachesToEveryTraversalReference verifies postgres traversal telemetry attaches to every traversal reference behavior.
func TestPostgresTraversalTelemetryAttachesToEveryTraversalReference(t *testing.T) {
	metrics := PostgresPlanMetrics{
		PlanNodes: []PostgresPlanNodeMetric{{
			NodeType:    "Recursive Union",
			ActualRows:  2,
			ActualLoops: 1,
		}},
		Provenance: map[string]string{},
	}
	record := CaseResult{PostgresReferences: []PostgresReferenceResult{
		{
			Name:                         "forward",
			Architecture:                 "EXPANSION-STEPWISE-FORWARD-SQL",
			ImplementationID:             "forward_v1",
			PostgresMetrics:              &metrics,
			traversalTelemetryParameters: map[string]any{},
		},
		{
			Name:                         "reverse",
			Architecture:                 "EXPANSION-SUFFIX-SEEDED-REVERSE",
			ImplementationID:             "reverse_v1",
			PostgresMetrics:              &metrics,
			traversalTelemetryParameters: map[string]any{},
		},
	}}
	runner := postgresSQLRunner{
		traversalTelemetry: postgresTraversalTelemetrySummary,
		backendPID:         "9123",
	}

	require.NoError(t, runner.attachPostgresTraversalTelemetry(t.Context(), &record, nil))
	require.Len(t, record.PostgresReferences, 2)
	for _, reference := range record.PostgresReferences {
		require.NotNil(t, reference.TraversalTelemetry)
		require.Equal(t, TraversalTelemetryLevelSummary, reference.TraversalTelemetry.Level)
		require.Equal(t, reference.Architecture, reference.TraversalTelemetry.Summary.RuntimeIdentity)
		require.Nil(t, reference.TraversalTelemetry.Diagnostic)
		require.NoError(t, reference.TraversalTelemetry.Validate())
	}
}

// TestPostgresTraversalTelemetrySkipsNonTraversalReferenceBoundaries verifies postgres traversal telemetry skips non traversal reference boundaries behavior.
func TestPostgresTraversalTelemetrySkipsNonTraversalReferenceBoundaries(t *testing.T) {
	metrics := PostgresPlanMetrics{
		PlanNodes: []PostgresPlanNodeMetric{{
			NodeType:    "Result",
			ActualRows:  1,
			ActualLoops: 1,
		}},
		Provenance: map[string]string{},
	}
	for _, architecture := range []string{"component_probe", "protocol", "root_validation", "root_adjacency", "factored_suffix"} {
		reference := PostgresReferenceResult{
			Architecture:     architecture,
			ImplementationID: architecture + "_v1",
			PostgresMetrics:  &metrics,
		}
		telemetry, err := buildPostgresReferenceTraversalTelemetry(reference, nil, "9123", TraversalTelemetryLevelDiagnostic)
		require.NoError(t, err)
		require.Nil(t, telemetry, architecture)
	}
}

// TestParseConfigValidatesPostgresTraversalTelemetryMode verifies parse config validates postgres traversal telemetry mode behavior.
func TestParseConfigValidatesPostgresTraversalTelemetryMode(t *testing.T) {
	cfg, err := parseConfig([]string{"-postgres-traversal-telemetry", "summary"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, postgresTraversalTelemetrySummary, cfg.PostgresTraversalTelemetry)

	cfg, err = parseConfig([]string{"-postgres-traversal-telemetry", "diagnostic"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, postgresTraversalTelemetryDiagnostic, cfg.PostgresTraversalTelemetry)

	_, err = parseConfig([]string{"-postgres-traversal-telemetry", "unknown"}, func(string) string { return "" })
	require.ErrorContains(t, err, "must be off, summary, or diagnostic")

	_, err = parseConfig([]string{"-postgres-traversal-telemetry", "diagnostic", "-pool-size", "2"}, func(string) string { return "" })
	require.ErrorContains(t, err, "requires pool-size 1")

	cfg, err = parseConfig([]string{"-postgres-expansion-orientation-shadow"}, func(string) string { return "" })
	require.NoError(t, err)
	require.True(t, cfg.PostgresExpansionOrientationShadow)

	_, err = parseConfig([]string{"-postgres-expansion-orientation-shadow", "-postgres-force-expansion-search", "EXPANSION-SUFFIX-SEEDED-REVERSE"}, func(string) string { return "" })
	require.ErrorContains(t, err, "mutually exclusive")
}

// TestParseConfigAcceptsExplicitOrientationProbeV2MeasurementModes verifies parse config accepts explicit orientation probe v2 measurement modes behavior.
func TestParseConfigAcceptsExplicitOrientationProbeV2MeasurementModes(t *testing.T) {
	for _, mode := range [][]string{
		{"-postgres-expansion-orientation-shadow"},
		{"-postgres-expansion-orientation-tournament"},
	} {
		args := append(append([]string(nil), mode...),
			"-postgres-expansion-orientation-policy", "orientation-probe-v2",
			"-postgres-repeatable-read",
			"-postgres-traversal-telemetry", "summary",
		)
		cfg, err := parseConfig(args, func(string) string { return "" })
		require.NoError(t, err, mode)
		require.Equal(t, "orientation-probe-v2", cfg.PostgresExpansionOrientationPolicy)
		require.True(t, cfg.PostgresRepeatableRead)
		require.Equal(t, postgresTraversalTelemetrySummary, cfg.PostgresTraversalTelemetry)
	}

	for _, args := range [][]string{
		{"-postgres-expansion-orientation-policy", "orientation-probe-v2", "-postgres-repeatable-read", "-postgres-traversal-telemetry", "summary"},
		{"-postgres-expansion-orientation-shadow", "-postgres-expansion-orientation-policy", "orientation-probe-v3", "-postgres-repeatable-read", "-postgres-traversal-telemetry", "summary"},
		{"-postgres-expansion-orientation-shadow", "-postgres-expansion-orientation-tournament", "-postgres-repeatable-read"},
		{"-postgres-expansion-orientation-shadow", "-postgres-expansion-orientation-policy", "orientation-probe-v2", "-postgres-traversal-telemetry", "summary"},
		{"-postgres-expansion-orientation-shadow", "-postgres-expansion-orientation-policy", "orientation-probe-v2", "-postgres-repeatable-read"},
		{"-postgres-expansion-orientation-tournament"},
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

func TestParseConfigValidatesSuffixReverseGuardMeasurementMode(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-postgres-expansion-suffix-reverse-guard",
		"-postgres-repeatable-read",
		"-postgres-traversal-telemetry", "diagnostic",
		"-postgres-suffix-guard-suffix-limit", "64",
		"-postgres-suffix-guard-state-limit", "128",
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.True(t, cfg.PostgresExpansionSuffixReverseGuard)
	require.Equal(t, int64(64), cfg.PostgresSuffixGuardSuffixLimit)
	require.Equal(t, int64(128), cfg.PostgresSuffixGuardStateLimit)

	for _, args := range [][]string{
		{"-postgres-expansion-suffix-reverse-guard"},
		{"-postgres-expansion-suffix-reverse-guard", "-postgres-repeatable-read", "-postgres-traversal-telemetry", "summary"},
		{"-postgres-suffix-guard-state-limit", "1"},
		{"-postgres-expansion-suffix-reverse-guard", "-postgres-repeatable-read", "-postgres-traversal-telemetry", "diagnostic", "-postgres-force-expansion-search", "EXPANSION-SUFFIX-SEEDED-REVERSE"},
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

func TestParseConfigValidatesSuffixReverseRetryMeasurementMode(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-postgres-expansion-suffix-reverse-retry",
		"-postgres-repeatable-read",
		"-postgres-traversal-telemetry", "diagnostic",
		"-postgres-suffix-guard-suffix-limit", "64",
		"-postgres-suffix-guard-state-limit", "128",
		"-postgres-suffix-retry-output-row-limit", "256",
		"-postgres-suffix-retry-output-bytes-limit", "1048576",
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.True(t, cfg.PostgresExpansionSuffixReverseRetry)
	require.Equal(t, int64(256), cfg.PostgresSuffixRetryOutputRowLimit)
	require.Equal(t, int64(1048576), cfg.PostgresSuffixRetryOutputBytesLimit)

	for _, args := range [][]string{
		{"-postgres-expansion-suffix-reverse-retry"},
		{"-postgres-expansion-suffix-reverse-retry", "-postgres-repeatable-read", "-postgres-traversal-telemetry", "summary"},
		{"-postgres-expansion-suffix-reverse-retry", "-postgres-repeatable-read", "-postgres-traversal-telemetry", "diagnostic", "-pool-size", "2"},
		{"-postgres-suffix-retry-output-row-limit", "1"},
		{"-postgres-expansion-suffix-reverse-retry", "-postgres-expansion-suffix-reverse-guard", "-postgres-repeatable-read", "-postgres-traversal-telemetry", "diagnostic"},
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

// bidirectionalCaseTelemetry prepares or inspects test evidence for bidirectional case telemetry.
func bidirectionalCaseTelemetry(t *testing.T, level TraversalTelemetryLevel) *TraversalExecutionTelemetry {
	t.Helper()
	outcome := translate.TargetLoweringOutcome{
		Family:            "SP",
		Candidate:         "SP-B2-C-MIN-LEVEL-D",
		Selected:          "SP-B2-C-MIN-LEVEL-D",
		Applied:           "SP-B2-C-MIN-LEVEL-D",
		Fallback:          "SP-S4-C-D",
		PlannedCandidates: []string{"SP-B2-C-MIN-LEVEL-D", "SP-S4-C-D"},
		Scheduler:         "smaller_current_level",
		SelectorVersion:   "sp-tool-v1",
		StateLimit:        100,
		FrontierLimit:     50,
		PredecessorLimit:  25,
	}
	metrics := PostgresPlanMetrics{
		PlanNodes: []PostgresPlanNodeMetric{{
			NodeType:     "Function Scan",
			FunctionName: "shortest_path_b2_smaller_current_level",
			ActualRows:   1,
			ActualLoops:  1,
		}},
		Provenance: map[string]string{},
	}
	telemetry, err := buildPostgresCaseTraversalTelemetry(
		translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}},
		metrics,
		"9123",
		level,
	)
	require.NoError(t, err)
	require.NotNil(t, telemetry)
	return telemetry
}

// validBidirectionalDiagnosticDocument returns a self-consistent one-path bidirectional runtime receipt.
func validBidirectionalDiagnosticDocument(invocationID string) *postgresBidirectionalDiagnosticDocument {
	return &postgresBidirectionalDiagnosticDocument{
		SchemaVersion:    1,
		InvocationID:     invocationID,
		Scheduler:        "smaller_current_level",
		StateLimit:       traversalTelemetryPointer(int64(100)),
		FrontierLimit:    traversalTelemetryPointer(int64(50)),
		PredecessorLimit: traversalTelemetryPointer(int64(25)),
		SearchCalls:      traversalTelemetryPointer(int64(1)),
		RuntimeBranch:    "bidirectional_search",
		Overflowed:       traversalTelemetryPointer(false),
		FallbackExecuted: traversalTelemetryPointer(false),
		Counters: &postgresBidirectionalDiagnosticCounts{
			SchedulerActions:  traversalTelemetryPointer(int64(2)),
			CandidateEdges:    traversalTelemetryPointer(int64(7)),
			DistinctNewNodes:  traversalTelemetryPointer(int64(5)),
			SeenPeak:          traversalTelemetryPointer(int64(6)),
			FrontierPeak:      traversalTelemetryPointer(int64(3)),
			QueuePeak:         traversalTelemetryPointer(int64(3)),
			PredecessorPeak:   traversalTelemetryPointer(int64(4)),
			MeetingCandidates: traversalTelemetryPointer(int64(1)),
			FrozenDistance:    traversalTelemetryPointer(int64(3)),
			WitnessRows:       traversalTelemetryPointer(int64(1)),
			Levels: []postgresBidirectionalDiagnosticLevel{{
				SearchID:          traversalTelemetryPointer(int64(1)),
				ActionIndex:       traversalTelemetryPointer(int64(1)),
				Side:              "forward",
				Action:            "expand_level",
				Depth:             traversalTelemetryPointer(int64(1)),
				FrontierRows:      traversalTelemetryPointer(int64(2)),
				CandidateEdges:    traversalTelemetryPointer(int64(7)),
				DistinctNewNodes:  traversalTelemetryPointer(int64(5)),
				SeenRows:          traversalTelemetryPointer(int64(6)),
				QueueRows:         traversalTelemetryPointer(int64(3)),
				PredecessorRows:   traversalTelemetryPointer(int64(4)),
				MeetingCandidates: traversalTelemetryPointer(int64(1)),
			}},
		},
		Calls: []postgresBidirectionalDiagnosticCall{{
			SearchID:          traversalTelemetryPointer(int64(1)),
			SourceID:          traversalTelemetryPointer(int64(10)),
			TargetID:          traversalTelemetryPointer(int64(20)),
			RuntimeBranch:     "bidirectional_search",
			SchedulerActions:  traversalTelemetryPointer(int64(2)),
			CandidateEdges:    traversalTelemetryPointer(int64(7)),
			DistinctNewNodes:  traversalTelemetryPointer(int64(5)),
			SeenPeak:          traversalTelemetryPointer(int64(6)),
			FrontierPeak:      traversalTelemetryPointer(int64(3)),
			QueuePeak:         traversalTelemetryPointer(int64(3)),
			PredecessorPeak:   traversalTelemetryPointer(int64(4)),
			MeetingCandidates: traversalTelemetryPointer(int64(1)),
			FrozenDistance:    traversalTelemetryPointer(int64(3)),
			WitnessRows:       traversalTelemetryPointer(int64(1)),
			Overflowed:        traversalTelemetryPointer(false),
			FallbackExecuted:  traversalTelemetryPointer(false),
		}},
	}
}

// bidirectionalASPCaseTelemetry prepares or inspects test evidence for bidirectional asp case telemetry.
func bidirectionalASPCaseTelemetry(t *testing.T) *TraversalExecutionTelemetry {
	t.Helper()
	outcome := translate.TargetLoweringOutcome{
		Family:            "ASP",
		Candidate:         "ASP-B2-DAG-MIN-LEVEL",
		Selected:          "ASP-B2-DAG-MIN-LEVEL",
		Applied:           "ASP-B2-DAG-MIN-LEVEL",
		Fallback:          "ASP-A1-DAG",
		PlannedCandidates: []string{"ASP-B2-DAG-MIN-LEVEL", "ASP-A1-DAG"},
		Scheduler:         "smaller_current_level",
		SelectorVersion:   "asp-tool-v1",
		StateLimit:        100,
		FrontierLimit:     50,
		PredecessorLimit:  25,
		EnumerationLimit:  1000,
		OutputBytesLimit:  4096,
	}
	metrics := PostgresPlanMetrics{
		PlanNodes: []PostgresPlanNodeMetric{{
			NodeType:     "Function Scan",
			FunctionName: "all_shortest_paths_b2_smaller_current_level",
			ActualRows:   1,
			ActualLoops:  1,
		}},
		Provenance: map[string]string{},
	}
	telemetry, err := buildPostgresCaseTraversalTelemetry(
		translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelDiagnostic,
	)
	require.NoError(t, err)
	require.NotNil(t, telemetry)
	return telemetry
}

// validBidirectionalAllShortestDiagnosticDocument returns a self-consistent all-shortest runtime receipt.
func validBidirectionalAllShortestDiagnosticDocument(invocationID string) *postgresBidirectionalAllShortestDiagnosticDocument {
	base := validBidirectionalDiagnosticDocument(invocationID)
	counts := &postgresBidirectionalAllShortestDiagnosticCounts{
		SchedulerActions:              base.Counters.SchedulerActions,
		CandidateEdges:                base.Counters.CandidateEdges,
		DistinctNewNodes:              base.Counters.DistinctNewNodes,
		SeenPeak:                      base.Counters.SeenPeak,
		FrontierPeak:                  base.Counters.FrontierPeak,
		QueuePeak:                     base.Counters.QueuePeak,
		PredecessorPeak:               base.Counters.PredecessorPeak,
		MeetingCandidates:             base.Counters.MeetingCandidates,
		FrozenDistance:                base.Counters.FrozenDistance,
		WitnessRows:                   base.Counters.WitnessRows,
		Levels:                        base.Counters.Levels,
		SameDepthPredecessorAdditions: traversalTelemetryPointer(int64(5)),
		MeetingNodes:                  traversalTelemetryPointer(int64(2)),
		CutDepth:                      traversalTelemetryPointer(int64(3)),
		PathCountEstimate:             traversalTelemetryPointer(int64(12)),
		PathCountSaturated:            traversalTelemetryPointer(false),
		EnumeratedCandidates:          traversalTelemetryPointer(int64(13)),
		DuplicateRejects:              traversalTelemetryPointer(int64(1)),
		OutputPaths:                   traversalTelemetryPointer(int64(12)),
		OutputEdgeCells:               traversalTelemetryPointer(int64(36)),
		OutputBytes:                   traversalTelemetryPointer(int64(384)),
	}
	call := postgresBidirectionalAllShortestDiagnosticCall{
		SearchID:                      base.Calls[0].SearchID,
		SourceID:                      base.Calls[0].SourceID,
		TargetID:                      base.Calls[0].TargetID,
		RuntimeBranch:                 base.Calls[0].RuntimeBranch,
		SchedulerActions:              base.Calls[0].SchedulerActions,
		CandidateEdges:                base.Calls[0].CandidateEdges,
		DistinctNewNodes:              base.Calls[0].DistinctNewNodes,
		SeenPeak:                      base.Calls[0].SeenPeak,
		FrontierPeak:                  base.Calls[0].FrontierPeak,
		QueuePeak:                     base.Calls[0].QueuePeak,
		PredecessorPeak:               base.Calls[0].PredecessorPeak,
		MeetingCandidates:             base.Calls[0].MeetingCandidates,
		FrozenDistance:                base.Calls[0].FrozenDistance,
		WitnessRows:                   base.Calls[0].WitnessRows,
		SameDepthPredecessorAdditions: counts.SameDepthPredecessorAdditions,
		MeetingNodes:                  counts.MeetingNodes,
		CutDepth:                      counts.CutDepth,
		PathCountEstimate:             counts.PathCountEstimate,
		PathCountSaturated:            counts.PathCountSaturated,
		EnumeratedCandidates:          counts.EnumeratedCandidates,
		DuplicateRejects:              counts.DuplicateRejects,
		OutputPaths:                   counts.OutputPaths,
		OutputEdgeCells:               counts.OutputEdgeCells,
		OutputBytes:                   counts.OutputBytes,
		Overflowed:                    base.Calls[0].Overflowed,
		FallbackExecuted:              base.Calls[0].FallbackExecuted,
	}
	return &postgresBidirectionalAllShortestDiagnosticDocument{
		SchemaVersion:    1,
		InvocationID:     invocationID,
		Scheduler:        "smaller_current_level",
		StateLimit:       traversalTelemetryPointer(int64(100)),
		FrontierLimit:    traversalTelemetryPointer(int64(50)),
		PredecessorLimit: traversalTelemetryPointer(int64(25)),
		EnumerationLimit: traversalTelemetryPointer(int64(1000)),
		OutputBytesLimit: traversalTelemetryPointer(int64(4096)),
		SearchCalls:      traversalTelemetryPointer(int64(1)),
		RuntimeBranch:    "bidirectional_search",
		Overflowed:       traversalTelemetryPointer(false),
		FallbackExecuted: traversalTelemetryPointer(false),
		Counters:         counts,
		Calls:            []postgresBidirectionalAllShortestDiagnosticCall{call},
	}
}
