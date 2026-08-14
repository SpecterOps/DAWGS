// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build manual_integration

package main

import (
	"context"
	"net/url"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

// TestPostgreSQLSPI2GuardedDistancePlanAttributionAndFallback executes only
// already-open SP-I2 training cases. It proves reachable and no-path candidate
// receipts, then lowers diagnostic caps to force the same-statement exact S4
// arm and compares every public observation with an explicit S4 run.
func TestPostgreSQLSPI2GuardedDistancePlanAttributionAndFallback(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	connectionURL, err := url.Parse(connection)
	require.NoError(t, err)
	if connectionURL.Scheme != "postgres" && connectionURL.Scheme != "postgresql" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	const (
		reachableCase = "GSP-I2-V1-TRAIN-D03-RI064-FI032-full"
		overflowCase  = "GSP-I2-V1-TRAIN-D16-RI256-FI512-full"
		noPathCase    = "GSP-I2-V1-TRAIN-D16-RI256-FI512-disconnected"
		cycleCase     = "GSP-I2-V1-TRAIN-cycle-control"
	)
	selected, _, err := selectScaleCorpus(corpus, CorpusSelectors{Cases: []string{reachableCase, overflowCase, noPathCase, cycleCase}})
	require.NoError(t, err)
	require.Len(t, selected.Cases, 4)
	for _, testCase := range selected.Cases {
		require.Equal(t, "training", testCase.Shape.QualificationSplit)
		require.NotContains(t, testCase.Tags, spI2HoldoutTag)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, selected, 1, 1, nil, false, nil, "", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
	runner.repeatableRead = true
	runner.traversalTelemetry = postgresTraversalTelemetryDiagnostic

	run := func(corpus ScaleCorpus, options translate.ToolOptions) []CaseResult {
		runner.toolOptions = options
		records, err := runner.Run(ctx, 0, 1, corpus)
		require.NoError(t, err)
		for _, record := range records {
			require.Equal(t, StatusOK, record.Status, record.Error)
		}
		return records
	}
	byName := func(records []CaseResult) map[string]CaseResult {
		indexed := make(map[string]CaseResult, len(records))
		for _, record := range records {
			indexed[record.Name] = record
		}
		return indexed
	}

	baseline := byName(run(selected, translate.ToolOptions{
		ForceShortestPathExecutor: optimize.ShortestPathExecutorS4CanonicalDistance,
	}))
	candidate := run(selected, translate.ToolOptions{
		ForceShortestPathExecutor: optimize.ShortestPathExecutorI2GuardedDistance,
	})
	require.Len(t, candidate, 4)
	for _, record := range candidate {
		exact := baseline[record.Name]
		require.Equal(t, exact.RowCount, record.RowCount)
		require.True(t, slices.Equal(exact.ObservedRows, record.ObservedRows))
		require.NotNil(t, record.TraversalTelemetry)
		require.NoError(t, record.TraversalTelemetry.Validate())

		summary := record.TraversalTelemetry.Summary
		require.Equal(t, optimize.ShortestPathPolicyI2DistanceGuardedV1, summary.EmittedIdentity)
		require.Equal(t, string(optimize.ShortestPathExecutorI2GuardedDistance), summary.RuntimeIdentity)
		require.False(t, *summary.Overflow)
		require.False(t, *summary.FallbackExecuted)
		if record.Name == noPathCase {
			require.Equal(t, "inline_canonical_distance_no_path", summary.RuntimeBranch)
		} else {
			require.Equal(t, "inline_canonical_distance", summary.RuntimeBranch)
		}

		diagnostic := record.TraversalTelemetry.Diagnostic
		require.Equal(t, TraversalTelemetryCounterStatusComplete, diagnostic.CounterStatus)
		require.NotNil(t, diagnostic.PlanReplay)
		counters := diagnostic.PlanReplay.Counters
		if record.Name == cycleCase {
			require.Equal(t, int64(2), counters["sp_i2_distance_rows"])
		}
		require.Equal(t, int64(1), counters["sp_i2_candidate_marker_rows"])
		require.Zero(t, counters["sp_i2_fallback_marker_rows"])
		require.Equal(t, int64(1), counters["sp_i2_candidate_executor_loops"])
		require.Zero(t, counters["sp_i2_fallback_executor_loops"])
		require.Zero(t, counters["sp_i2_fallback_branch_rows"])
		require.Equal(t, record.RowCount, counters["sp_i2_output_rows"])
		gateCase := &ResourceGateCase{}
		appendInlineDistanceAttributionReasons(gateCase, record.TraversalTelemetry)
		require.Empty(t, gateCase.Reasons)
	}

	reduced, _, err := selectScaleCorpus(corpus, CorpusSelectors{Cases: []string{overflowCase}})
	require.NoError(t, err)
	require.Len(t, reduced.Cases, 1)
	fallback := run(reduced, translate.ToolOptions{
		ForceShortestPathExecutor:    optimize.ShortestPathExecutorI2GuardedDistance,
		GuardedDistanceStateLimit:    10,
		GuardedDistanceFrontierLimit: 10,
	})
	require.Len(t, fallback, 1)
	record := fallback[0]
	exact := baseline[record.Name]
	require.Equal(t, exact.RowCount, record.RowCount)
	require.True(t, slices.Equal(exact.ObservedRows, record.ObservedRows))
	require.NotNil(t, record.TraversalTelemetry)
	require.NoError(t, record.TraversalTelemetry.Validate())

	summary := record.TraversalTelemetry.Summary
	require.Equal(t, optimize.ShortestPathPolicyI2DistanceGuardedV1, summary.EmittedIdentity)
	require.Equal(t, string(optimize.ShortestPathExecutorS4CanonicalDistance), summary.RuntimeIdentity)
	require.Equal(t, "exact_s4_distance_fallback", summary.RuntimeBranch)
	require.True(t, *summary.Overflow)
	require.True(t, *summary.FallbackExecuted)
	diagnostic := record.TraversalTelemetry.Diagnostic
	require.Equal(t, TraversalTelemetryCounterStatusComplete, diagnostic.CounterStatus)
	counters := diagnostic.PlanReplay.Counters
	require.Zero(t, counters["sp_i2_candidate_marker_rows"])
	require.Equal(t, int64(1), counters["sp_i2_fallback_marker_rows"])
	require.Zero(t, counters["sp_i2_candidate_executor_loops"])
	require.Equal(t, int64(1), counters["sp_i2_fallback_executor_loops"])
	require.Zero(t, counters["sp_i2_candidate_branch_rows"])
	require.Equal(t, record.RowCount, counters["sp_i2_output_rows"])
	gateCase := &ResourceGateCase{}
	appendInlineDistanceAttributionReasons(gateCase, record.TraversalTelemetry)
	require.Empty(t, gateCase.Reasons)

	chains := runtimeReceiptChains(record.Stats.Samples)
	require.Len(t, chains, 1)
	warm := operationalWarmSamples(record)
	require.Len(t, warm, 1)
	require.NoError(t, validateRuntimeReceiptEvents(chains[0], warm[0].RuntimeIdentity, warm[0].RuntimeBranch, warm[0].FallbackExecuted))
	require.True(t, receiptChainContainsIdentity(chains[0], string(optimize.ShortestPathExecutorS4CanonicalDistance), true))
}
