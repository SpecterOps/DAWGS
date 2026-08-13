// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

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

func TestPostgresTraversalTelemetryRequiresExactlyOneSingletonSearchCall(t *testing.T) {
	telemetry := bidirectionalCaseTelemetry(t, TraversalTelemetryLevelDiagnostic)
	document := validBidirectionalDiagnosticDocument(telemetry.Diagnostic.InvocationID)
	document.SearchCalls = traversalTelemetryPointer(int64(2))
	document.Calls = append(document.Calls, document.Calls[0])
	document.Calls[1].SearchID = traversalTelemetryPointer(int64(2))

	err := applyBidirectionalTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "9123")
	require.ErrorContains(t, err, "exactly one search call")
}

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

func TestPostgresTraversalTelemetryCompletesASPHydrationFromInvocationAndPlanEvidence(t *testing.T) {
	telemetry := bidirectionalASPCaseTelemetry(t)
	document := validBidirectionalAllShortestDiagnosticDocument(telemetry.Diagnostic.InvocationID)
	require.NoError(t, applyBidirectionalAllShortestTraversalDiagnostic(telemetry, document, telemetry.Diagnostic.InvocationID, "9123"))
	metrics := PostgresPlanMetrics{HydrationRows: 48, HydrationLoops: 12, PlanNodes: []PostgresPlanNodeMetric{{
		NodeType: "Index Scan", RelationName: "node", Alias: "hydrated_nodes", ActualRows: 4, ActualLoops: 12, ActualTotalMS: .25,
	}}}
	enrichBidirectionalHydrationTelemetry(telemetry, document.Counters.OutputPaths, document.Counters.OutputEdgeCells, []string{`["p1"]`, `["p2"]`}, metrics)
	require.NoError(t, telemetry.Validate())
	require.Equal(t, TraversalTelemetryCounterStatusComplete, telemetry.Diagnostic.CounterStatus)
	require.Equal(t, int64(12), *telemetry.Diagnostic.Counters.Hydration.PathCount)
	require.Equal(t, int64(36), *telemetry.Diagnostic.Counters.Hydration.EdgeLookups)
	require.Equal(t, int64(48), *telemetry.Diagnostic.Counters.Hydration.NodeLookups)
}

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

func TestPostgresTraversalTelemetryLeavesNonBidirectionalHiddenFunctionsUnavailable(t *testing.T) {
	metrics := PostgresPlanMetrics{
		PlanNodes:  []PostgresPlanNodeMetric{{NodeType: "Function Scan", FunctionName: "all_shortest_paths_dag", ActualLoops: 1}},
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
	metrics := PostgresPlanMetrics{Provenance: map[string]string{}, PlanNodes: []PostgresPlanNodeMetric{
		{NodeType: "Result", SubplanName: "CTE s5_orientation_executed_candidate", ActualRows: 1, ActualLoops: 1},
		{NodeType: "Result", SubplanName: "CTE s5_orientation_executed_incumbent", ActualRows: 0, ActualLoops: 1},
	}}

	telemetry, err := buildPostgresCaseTraversalTelemetry(
		translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelDiagnostic,
	)
	require.NoError(t, err)
	require.NoError(t, telemetry.Validate())
	require.Equal(t, []TraversalTelemetryFamily{TraversalTelemetryFamilyOrientation}, telemetry.Diagnostic.RequiredFamilies)
	require.Equal(t, string(optimize.ExpansionSearchEndpointSeededReverse), telemetry.Summary.RuntimeIdentity)
}

func TestPostgresTraversalTelemetryCompletesGuardedInlineASPCounters(t *testing.T) {
	outcome := translate.TargetLoweringOutcome{
		Family: "ASP", Candidate: "ASP-I1-U-DAG+MAT-M0", Selected: "ASP-I1-U-DAG+MAT-M0", Applied: "ASP-I1-U-DAG+MAT-M0",
		Fallback: "ASP-A1-DAG", PlannedCandidates: []string{"ASP-A1-DAG", "ASP-I1-U-DAG+MAT-M0"},
		EmittedCandidates: []string{"ASP-I1-U-DAG+MAT-M0", "ASP-A1-DAG"}, EmittedPolicy: "asp-i1-guarded-v1",
		SelectionMode: "production_canary", SelectorVersion: "asp-i1-canary-v1", ExecutionBoundary: "guarded_dual_arm",
		ObservationMode: "all_paths", StateLimit: 10, PredecessorLimit: 20, EnumerationLimit: 30, OutputBytesLimit: 1000,
	}
	metrics := PostgresPlanMetrics{Provenance: map[string]string{}, HydrationRows: 4, HydrationLoops: 2, PlanNodes: []PostgresPlanNodeMetric{
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
	}}
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

func TestPostgresTraversalTelemetryCompletesGuardedInlineCanonicalSPCounters(t *testing.T) {
	outcome := translate.TargetLoweringOutcome{
		Family: "SP", Candidate: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		Selected: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		Applied:  string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		Fallback: string(optimize.ShortestPathExecutorS4CanonicalWitness),
		PlannedCandidates: []string{
			string(optimize.ShortestPathExecutorS4CanonicalWitness),
			string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		},
		EmittedCandidates: []string{
			string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
			string(optimize.ShortestPathExecutorS4CanonicalWitness),
		},
		EmittedPolicy: optimize.ShortestPathPolicyI1CanonicalGuardedV1,
		SelectionMode: "production_canary", SelectorVersion: "sp-i1-canary-v1", ExecutionBoundary: "guarded_dual_arm",
		ObservationMode: "one_path", StateLimit: 10, PredecessorLimit: 20, EnumerationLimit: 30, OutputBytesLimit: 1000,
	}

	tests := []struct {
		name             string
		candidateMarker  int64
		fallbackMarker   int64
		outputRows       int64
		distanceRows     int64
		expectedIdentity string
		expectedBranch   string
		expectedFallback bool
	}{
		{name: "candidate witness", candidateMarker: 1, outputRows: 1, distanceRows: 3,
			expectedIdentity: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness), expectedBranch: "inline_canonical_witness"},
		{name: "candidate no path", candidateMarker: 1, distanceRows: 3,
			expectedIdentity: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness), expectedBranch: "inline_canonical_no_path"},
		{name: "exact S4 fallback", fallbackMarker: 1, outputRows: 1, distanceRows: 11,
			expectedIdentity: string(optimize.ShortestPathExecutorS4CanonicalWitness), expectedBranch: "exact_s4_fallback", expectedFallback: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metrics := PostgresPlanMetrics{Provenance: map[string]string{}, HydrationRows: test.outputRows, HydrationLoops: test.outputRows, PlanNodes: []PostgresPlanNodeMetric{
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
			}}
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

func TestPostgresTraversalTelemetryRejectsEveryMissingInlinePredecessorCounter(t *testing.T) {
	outcome := translate.TargetLoweringOutcome{
		Family: "SP", Candidate: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		Selected: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		Applied:  string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		Fallback: string(optimize.ShortestPathExecutorS4CanonicalWitness),
		PlannedCandidates: []string{
			string(optimize.ShortestPathExecutorS4CanonicalWitness),
			string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		},
		EmittedCandidates: []string{
			string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
			string(optimize.ShortestPathExecutorS4CanonicalWitness),
		},
		EmittedPolicy:   optimize.ShortestPathPolicyI1CanonicalGuardedV1,
		ObservationMode: "one_path", StateLimit: 10, PredecessorLimit: 20, EnumerationLimit: 30, OutputBytesLimit: 1000,
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

func TestPostgresTraversalPlanReplayUsesExactInlinePredecessorCTEBodies(t *testing.T) {
	metrics := PostgresPlanMetrics{Provenance: map[string]string{}, PlanNodes: []PostgresPlanNodeMetric{
		inlinePredecessorPlanNode("asp_i1_distance_bounded", 3, 1),
		{PlanNodeID: 500, NodeType: "Limit", SubplanName: "CTE prefix_asp_i1_distance_bounded", ActualRows: 77, ActualLoops: 1},
		{NodeType: "CTE Scan", CTEName: "asp_i1_distance_bounded", Alias: "asp_i1_distance_bounded", ActualRows: 99, ActualLoops: 7},
		inlinePredecessorPlanNode("asp_i1_candidate_rows", 0, 1),
		{NodeType: "CTE Scan", CTEName: "asp_i1_candidate_rows", Alias: "asp_i1_candidate_rows", ActualRows: 10, ActualLoops: 5},
		inlinePredecessorPlanNode("asp_i1_fallback_rows", 0, 1),
		{NodeType: "CTE Scan", CTEName: "asp_i1_fallback_rows", Alias: "asp_i1_fallback_rows", ActualRows: 8, ActualLoops: 3},
		inlinePredecessorPlanNode("asp_i1_candidate_marker", 1, 1),
		inlinePredecessorPlanNode("asp_i1_fallback_marker", 0, 1),
		inlinePredecessorMarkerGateNode("candidate", 1, 1),
		inlinePredecessorMarkerGateNode("fallback", 0, 1),
		inlinePredecessorExecutorNode("candidate", 1),
		inlinePredecessorExecutorNode("fallback", 0),
	}}

	replay := postgresTraversalPlanReplay(metrics)
	require.Equal(t, int64(3), replay.Counters["asp_i1_distance_rows"])
	require.Equal(t, int64(0), replay.Counters["asp_i1_candidate_branch_rows"])
	require.Equal(t, int64(0), replay.Counters["asp_i1_fallback_branch_rows"])
	require.Equal(t, int64(1), replay.Counters["asp_i1_candidate_executor_loops"])
	require.Equal(t, int64(0), replay.Counters["asp_i1_fallback_executor_loops"])
}

func TestPostgresTraversalPlanReplayRejectsAmbiguousInlineBranchShape(t *testing.T) {
	t.Run("duplicate exact body", func(t *testing.T) {
		body := inlinePredecessorPlanNode("asp_i1_candidate_rows", 1, 1)
		duplicate := body
		duplicate.PlanNodeID = 99
		replay := postgresTraversalPlanReplay(PostgresPlanMetrics{Provenance: map[string]string{}, PlanNodes: []PostgresPlanNodeMetric{
			body, duplicate, inlinePredecessorPlanNode("asp_i1_candidate_marker", 1, 1),
			inlinePredecessorMarkerGateNode("candidate", 1, 1),
			inlinePredecessorExecutorNode("candidate", 1),
		}})
		_, branchPresent := replay.Counters["asp_i1_candidate_branch_rows"]
		_, executorPresent := replay.Counters["asp_i1_candidate_executor_loops"]
		require.False(t, branchPresent)
		require.False(t, executorPresent)
	})

	t.Run("wrong direct outer marker", func(t *testing.T) {
		body := inlinePredecessorPlanNode("asp_i1_candidate_rows", 1, 1)
		wrongMarker := inlinePredecessorMarkerGateNode("candidate", 1, 1)
		wrongMarker.CTEName = "asp_i1_fallback_marker"
		replay := postgresTraversalPlanReplay(PostgresPlanMetrics{Provenance: map[string]string{}, PlanNodes: []PostgresPlanNodeMetric{
			body, inlinePredecessorPlanNode("asp_i1_candidate_marker", 1, 1), wrongMarker, inlinePredecessorExecutorNode("candidate", 1),
		}})
		require.Equal(t, int64(1), replay.Counters["asp_i1_candidate_branch_rows"])
		_, executorPresent := replay.Counters["asp_i1_candidate_executor_loops"]
		require.False(t, executorPresent)
	})
}

func inlinePredecessorPlanNode(name string, rows, loops int64) PostgresPlanNodeMetric {
	return PostgresPlanNodeMetric{
		PlanNodeID: inlinePredecessorPlanNodeID(name), NodeType: "Result", SubplanName: "CTE " + name,
		ActualRows: rows, ActualLoops: loops,
	}
}

func inlinePredecessorExecutorNode(branch string, loops int64) PostgresPlanNodeMetric {
	bodyID := inlinePredecessorPlanNodeID("asp_i1_" + branch + "_rows")
	return PostgresPlanNodeMetric{
		PlanNodeID: bodyID + 100, ParentPlanNodeID: bodyID, ParentRelationship: "Inner",
		NodeType: "Result", Alias: "test_" + branch + "_executor", ActualLoops: loops,
	}
}

func inlinePredecessorMarkerGateNode(branch string, rows, loops int64) PostgresPlanNodeMetric {
	bodyID := inlinePredecessorPlanNodeID("asp_i1_" + branch + "_rows")
	return PostgresPlanNodeMetric{
		PlanNodeID: bodyID + 200, ParentPlanNodeID: bodyID, ParentRelationship: "Outer",
		NodeType: "CTE Scan", CTEName: "asp_i1_" + branch + "_marker", Alias: "test_" + branch + "_marker_gate",
		ActualRows: rows, ActualLoops: loops,
	}
}

func inlinePredecessorPlanNodeID(name string) int64 {
	ids := map[string]int64{
		"asp_i1_distance_bounded": 1, "asp_i1_predecessor_bounded": 2,
		"asp_i1_paths_bounded": 3, "asp_i1_shortest": 4,
		"asp_i1_candidate_marker": 5, "asp_i1_fallback_marker": 6,
		"asp_i1_candidate_rows": 7, "asp_i1_fallback_rows": 8,
	}
	return ids[name]
}

func TestPostgresTraversalTelemetryPrefersShortestExecutorOverAnalysisOutcomes(t *testing.T) {
	shortest := translate.TargetLoweringOutcome{TargetKind: "traversal", Family: "SP", Applied: "SP-B1-C-ALT-NODE-D"}
	outcome, found := singleTraversalOutcome([]translate.TargetLoweringOutcome{
		{TargetKind: "endpoint_resolution", Family: "endpoint_resolution", TraversalFamily: "SP", Applied: "ENDPOINT-RESOLUTION-INCUMBENT"},
		{TargetKind: "traversal_predicate", Family: "traversal_predicate", Applied: "TRAVERSAL-PREDICATE-INCUMBENT"},
		{TargetKind: "traversal", Family: "fixed_suffix_expansion", Applied: "EXPANSION-STEPWISE-FORWARD"},
		shortest,
	})
	require.True(t, found)
	require.Equal(t, shortest, outcome)
}

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
			{NodeType: "Result", SubplanName: "CTE s5_orientation_shadow_reverse", ActualRows: 1, ActualLoops: 1},
			{NodeType: "Result", SubplanName: "CTE s5_orientation_shadow_forward", ActualRows: 0, ActualLoops: 1},
			{NodeType: "Result", SubplanName: "CTE s5_orientation_executed_incumbent", ActualRows: 1, ActualLoops: 1},
			{NodeType: "Limit", SubplanName: "CTE s5_orientation_suffix_probe", ActualRows: 513, ActualLoops: 1},
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

func TestPostgresTraversalTelemetryUsesExactGuardedOrientationReceiptBranches(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		candidateRows    int64
		incumbentRows    int64
		rootProbeRows    int64
		runtimeIdentity  string
		runtimeBranch    string
		fallbackExecuted bool
		overflow         bool
	}{
		{
			name: "reverse candidate", candidateRows: 1, runtimeIdentity: string(optimize.ExpansionSearchSuffixSeededReverse),
			runtimeBranch: "suffix_seeded_reverse",
		},
		{
			name: "forward selection", incumbentRows: 1, runtimeIdentity: string(optimize.ExpansionSearchStepwiseForward),
			runtimeBranch: "exact_forward_incumbent",
		},
		{
			name: "overflow fallback", incumbentRows: 1, rootProbeRows: optimize.ExpansionSearchOrientationRootRowLimit + 1,
			runtimeIdentity: string(optimize.ExpansionSearchStepwiseForward), runtimeBranch: "exact_forward_incumbent",
			fallbackExecuted: true, overflow: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			outcome := translate.TargetLoweringOutcome{
				Family: "fixed_suffix_expansion", Candidate: string(optimize.ExpansionSearchSuffixSeededReverse),
				Selected: string(optimize.ExpansionSearchStepwiseForward), Applied: string(optimize.ExpansionSearchStepwiseForward),
				Fallback:          string(optimize.ExpansionSearchStepwiseForward),
				PlannedCandidates: []string{string(optimize.ExpansionSearchStepwiseForward), string(optimize.ExpansionSearchSuffixSeededReverse)},
				EmittedCandidates: []string{string(optimize.ExpansionSearchStepwiseForward), string(optimize.ExpansionSearchSuffixSeededReverse)},
				EmittedPolicy:     string(optimize.ExpansionSearchPolicyOrientationProbeV2), SelectorVersion: string(optimize.ExpansionSearchPolicyOrientationProbeV2),
				ExecutionBoundary: optimize.ExpansionSearchExecutionBoundaryGuardedDualArm,
				ProbeCaps:         &optimize.ExpansionSearchProbeCaps{RootRowLimit: optimize.ExpansionSearchOrientationRootRowLimit},
			}
			metrics := PostgresPlanMetrics{Provenance: map[string]string{}, PlanNodes: []PostgresPlanNodeMetric{
				{NodeType: "Result", SubplanName: "CTE s5_orientation_executed_candidate", ActualRows: testCase.candidateRows, ActualLoops: 1},
				{NodeType: "Result", SubplanName: "CTE s5_orientation_executed_incumbent", ActualRows: testCase.incumbentRows, ActualLoops: 1},
				{NodeType: "Limit", SubplanName: "CTE s5_orientation_root_probe", ActualRows: testCase.rootProbeRows, ActualLoops: 1},
			}}

			telemetry, err := buildPostgresCaseTraversalTelemetry(
				translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelSummary,
			)
			require.NoError(t, err)
			require.Equal(t, testCase.runtimeIdentity, telemetry.Summary.RuntimeIdentity)
			require.Equal(t, testCase.runtimeBranch, telemetry.Summary.RuntimeBranch)
			require.Equal(t, testCase.fallbackExecuted, *telemetry.Summary.FallbackExecuted)
			require.Equal(t, testCase.overflow, *telemetry.Summary.Overflow)
			require.NoError(t, validateRuntimeReceiptEvents([]RuntimeReceiptEvent{{
				Ordinal: 1, RuntimeIdentity: testCase.runtimeIdentity, RuntimeBranch: testCase.runtimeBranch,
				FallbackExecuted: testCase.fallbackExecuted,
			}}, telemetry.Summary.RuntimeIdentity, telemetry.Summary.RuntimeBranch, telemetry.Summary.FallbackExecuted))
		})
	}
}

func TestPostgresTraversalTelemetryUsesV2DepthWeightedDiagnosticScore(t *testing.T) {
	maximumDepth := int64(16)
	outcome := translate.TargetLoweringOutcome{
		Family: "fixed_suffix_expansion", Candidate: string(optimize.ExpansionSearchSuffixSeededReverse),
		Selected: string(optimize.ExpansionSearchStepwiseForward), Applied: string(optimize.ExpansionSearchStepwiseForward),
		Fallback: string(optimize.ExpansionSearchStepwiseForward), EmittedPolicy: string(optimize.ExpansionSearchPolicyOrientationProbeV2),
		SelectorVersion: string(optimize.ExpansionSearchPolicyOrientationProbeV2), MaximumDepth: &maximumDepth,
	}
	metrics := PostgresPlanMetrics{Provenance: map[string]string{}, PlanNodes: []PostgresPlanNodeMetric{
		{NodeType: "Limit", SubplanName: "CTE s5_orientation_root_probe", ActualRows: 2, ActualLoops: 1},
		{NodeType: "Limit", SubplanName: "CTE s5_orientation_forward_degree_probe", ActualRows: 8, ActualLoops: 1},
		{NodeType: "Result", SubplanName: "CTE s5_orientation_executed_incumbent", ActualRows: 1, ActualLoops: 1},
	}}
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

func TestPostgresTraversalTelemetryCompletesOrientationCountersFromNamedPlanNodes(t *testing.T) {
	outcome := translate.TargetLoweringOutcome{
		Family: "fixed_suffix_expansion", Candidate: "EXPANSION-SUFFIX-SEEDED-REVERSE",
		Selected: "EXPANSION-STEPWISE-FORWARD", Applied: "EXPANSION-STEPWISE-FORWARD", Fallback: "EXPANSION-STEPWISE-FORWARD",
		PlannedCandidates: []string{"EXPANSION-SUFFIX-SEEDED-REVERSE", "EXPANSION-STEPWISE-FORWARD"},
		EmittedCandidates: []string{"EXPANSION-SUFFIX-SEEDED-REVERSE", "EXPANSION-STEPWISE-FORWARD"},
		EmittedPolicy:     "orientation-probe-v1", SelectionMode: "production_canary", SelectorVersion: "orientation-probe-v1", StateLimit: 4096,
	}
	metrics := PostgresPlanMetrics{Provenance: map[string]string{}, PlanNodes: []PostgresPlanNodeMetric{
		{NodeType: "Limit", SubplanName: "CTE s5_orientation_root_probe", ActualRows: 2, ActualLoops: 1, ActualTotalMS: .01, Buffers: Buffers{SharedHit: 1}},
		{NodeType: "Limit", SubplanName: "CTE s5_orientation_suffix_probe", ActualRows: 5, ActualLoops: 1, ActualTotalMS: .02},
		{NodeType: "Aggregate", SubplanName: "CTE s5_orientation_boundaries", ActualRows: 3, ActualLoops: 1, ActualTotalMS: .01},
		{NodeType: "Limit", SubplanName: "CTE s5_orientation_forward_degree_probe", ActualRows: 8, ActualLoops: 1, ActualTotalMS: .01},
		{NodeType: "Limit", SubplanName: "CTE s5_orientation_reverse_degree_probe", ActualRows: 1, ActualLoops: 1, ActualTotalMS: .01},
		{NodeType: "Limit", SubplanName: "CTE s5_orientation_states", ActualRows: 4, ActualLoops: 1},
		{NodeType: "Result", SubplanName: "CTE s5_orientation_executed_candidate", ActualRows: 1, ActualLoops: 1},
		{NodeType: "Result", SubplanName: "CTE s5_orientation_executed_incumbent", ActualRows: 0, ActualLoops: 1},
		{NodeType: "Recursive Union", SubplanName: "CTE s5_orientation_reverse", ActualRows: 4, ActualLoops: 1},
		{NodeType: "Result", SubplanName: "CTE s5_orientation_decision", ActualRows: 1, ActualLoops: 1},
		// Consumer scans are deliberately repeated and must not inflate the
		// single materialization's row, loop, or branch attribution.
		{NodeType: "CTE Scan", CTEName: "s5_orientation_root_probe", Alias: "s5_orientation_root_probe", ActualRows: 2, ActualLoops: 3},
		{NodeType: "CTE Scan", CTEName: "s5_orientation_reverse_degree_probe", Alias: "s5_orientation_reverse_degree_probe", ActualRows: 1, ActualLoops: 7},
	}}
	telemetry, err := buildPostgresCaseTraversalTelemetry(translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelDiagnostic)
	require.NoError(t, err)
	enrichOrientationTraversalTelemetry(telemetry, metrics, 1, []string{`["path"]`}, 0)
	require.NoError(t, telemetry.Validate())
	require.Equal(t, TraversalTelemetryCounterStatusComplete, telemetry.Diagnostic.CounterStatus)
	require.Equal(t, int64(5), *telemetry.Diagnostic.Counters.Orientation.ReverseSeeds)
	require.Equal(t, int64(2), *telemetry.Diagnostic.Counters.Orientation.DuplicateSeeds)
	require.Equal(t, "reverse", telemetry.Diagnostic.Counters.Orientation.SelectedSide)
	require.Equal(t, int64(1), telemetry.Diagnostic.PlanReplay.Counters["orientation_root_probe_loops"])
	require.Equal(t, int64(1), telemetry.Diagnostic.PlanReplay.Counters["orientation_candidate_branch_loops"])
	require.Equal(t, int64(0), telemetry.Diagnostic.PlanReplay.Counters["orientation_incumbent_branch_loops"])
}

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

func TestPostgresTraversalTelemetryAttachesToEveryTraversalReference(t *testing.T) {
	metrics := PostgresPlanMetrics{PlanNodes: []PostgresPlanNodeMetric{{NodeType: "Recursive Union", ActualRows: 2, ActualLoops: 1}}, Provenance: map[string]string{}}
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

func TestPostgresTraversalTelemetrySkipsNonTraversalReferenceBoundaries(t *testing.T) {
	metrics := PostgresPlanMetrics{PlanNodes: []PostgresPlanNodeMetric{{NodeType: "Result", ActualRows: 1, ActualLoops: 1}}, Provenance: map[string]string{}}
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

func bidirectionalASPCaseTelemetry(t *testing.T) *TraversalExecutionTelemetry {
	t.Helper()
	outcome := translate.TargetLoweringOutcome{
		Family: "ASP", Candidate: "ASP-B2-DAG-MIN-LEVEL", Selected: "ASP-B2-DAG-MIN-LEVEL",
		Applied: "ASP-B2-DAG-MIN-LEVEL", Fallback: "ASP-A1-DAG",
		PlannedCandidates: []string{"ASP-B2-DAG-MIN-LEVEL", "ASP-A1-DAG"},
		Scheduler:         "smaller_current_level", SelectorVersion: "asp-tool-v1",
		StateLimit: 100, FrontierLimit: 50, PredecessorLimit: 25,
		EnumerationLimit: 1000, OutputBytesLimit: 4096,
	}
	metrics := PostgresPlanMetrics{
		PlanNodes:  []PostgresPlanNodeMetric{{NodeType: "Function Scan", FunctionName: "all_shortest_paths_b2_smaller_current_level", ActualRows: 1, ActualLoops: 1}},
		Provenance: map[string]string{},
	}
	telemetry, err := buildPostgresCaseTraversalTelemetry(
		translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}, metrics, "9123", TraversalTelemetryLevelDiagnostic,
	)
	require.NoError(t, err)
	require.NotNil(t, telemetry)
	return telemetry
}

func validBidirectionalAllShortestDiagnosticDocument(invocationID string) *postgresBidirectionalAllShortestDiagnosticDocument {
	base := validBidirectionalDiagnosticDocument(invocationID)
	counts := &postgresBidirectionalAllShortestDiagnosticCounts{
		SchedulerActions: base.Counters.SchedulerActions, CandidateEdges: base.Counters.CandidateEdges,
		DistinctNewNodes: base.Counters.DistinctNewNodes, SeenPeak: base.Counters.SeenPeak,
		FrontierPeak: base.Counters.FrontierPeak, QueuePeak: base.Counters.QueuePeak,
		PredecessorPeak: base.Counters.PredecessorPeak, MeetingCandidates: base.Counters.MeetingCandidates,
		FrozenDistance: base.Counters.FrozenDistance, WitnessRows: base.Counters.WitnessRows, Levels: base.Counters.Levels,
		SameDepthPredecessorAdditions: traversalTelemetryPointer(int64(5)), MeetingNodes: traversalTelemetryPointer(int64(2)),
		CutDepth: traversalTelemetryPointer(int64(3)), PathCountEstimate: traversalTelemetryPointer(int64(12)),
		PathCountSaturated: traversalTelemetryPointer(false), EnumeratedCandidates: traversalTelemetryPointer(int64(13)),
		DuplicateRejects: traversalTelemetryPointer(int64(1)), OutputPaths: traversalTelemetryPointer(int64(12)),
		OutputEdgeCells: traversalTelemetryPointer(int64(36)), OutputBytes: traversalTelemetryPointer(int64(384)),
	}
	call := postgresBidirectionalAllShortestDiagnosticCall{
		SearchID: base.Calls[0].SearchID, SourceID: base.Calls[0].SourceID, TargetID: base.Calls[0].TargetID,
		RuntimeBranch: base.Calls[0].RuntimeBranch, SchedulerActions: base.Calls[0].SchedulerActions,
		CandidateEdges: base.Calls[0].CandidateEdges, DistinctNewNodes: base.Calls[0].DistinctNewNodes,
		SeenPeak: base.Calls[0].SeenPeak, FrontierPeak: base.Calls[0].FrontierPeak, QueuePeak: base.Calls[0].QueuePeak,
		PredecessorPeak: base.Calls[0].PredecessorPeak, MeetingCandidates: base.Calls[0].MeetingCandidates,
		FrozenDistance: base.Calls[0].FrozenDistance, WitnessRows: base.Calls[0].WitnessRows,
		SameDepthPredecessorAdditions: counts.SameDepthPredecessorAdditions, MeetingNodes: counts.MeetingNodes,
		CutDepth: counts.CutDepth, PathCountEstimate: counts.PathCountEstimate, PathCountSaturated: counts.PathCountSaturated,
		EnumeratedCandidates: counts.EnumeratedCandidates, DuplicateRejects: counts.DuplicateRejects,
		OutputPaths: counts.OutputPaths, OutputEdgeCells: counts.OutputEdgeCells, OutputBytes: counts.OutputBytes,
		Overflowed: base.Calls[0].Overflowed, FallbackExecuted: base.Calls[0].FallbackExecuted,
	}
	return &postgresBidirectionalAllShortestDiagnosticDocument{
		SchemaVersion: 1, InvocationID: invocationID, Scheduler: "smaller_current_level",
		StateLimit: traversalTelemetryPointer(int64(100)), FrontierLimit: traversalTelemetryPointer(int64(50)),
		PredecessorLimit: traversalTelemetryPointer(int64(25)), EnumerationLimit: traversalTelemetryPointer(int64(1000)),
		OutputBytesLimit: traversalTelemetryPointer(int64(4096)), SearchCalls: traversalTelemetryPointer(int64(1)),
		RuntimeBranch: "bidirectional_search", Overflowed: traversalTelemetryPointer(false),
		FallbackExecuted: traversalTelemetryPointer(false), Counters: counts,
		Calls: []postgresBidirectionalAllShortestDiagnosticCall{call},
	}
}
