// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build manual_integration

package main

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/stretchr/testify/require"
)

// TestPostgreSQLSuffixReverseGuardPlanAttribution exercises one already-open
// full-path training case against a real PostgreSQL JSON EXPLAIN. It proves
// that the admitted reverse executor is marker-gated and that the exact
// forward fallback remains uninitialized. Protected holdout cases are never
// selected by this test.
func TestPostgreSQLSuffixReverseGuardPlanAttribution(t *testing.T) {
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
	const caseName = "GFSE-V3-TRAIN-Q4-C1-S1-productive_cycle_self_loop_path"
	selected, _, err := selectScaleCorpus(corpus, CorpusSelectors{Cases: []string{caseName}})
	require.NoError(t, err)
	require.Len(t, selected.Cases, 1)
	require.Equal(t, "training", selected.Cases[0].Shape.QualificationSplit)
	require.True(t, selected.Cases[0].Shape.PathMaterializationRequired)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, selected, 1, 1, nil, false, nil, "", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
	runner.repeatableRead = true
	runner.traversalTelemetry = postgresTraversalTelemetryDiagnostic
	runner.toolOptions.EnableExpansionSuffixReverseGuard = true

	records, err := runner.Run(ctx, 0, 1, selected)
	require.NoError(t, err)
	require.Len(t, records, 1)
	record := records[0]
	require.Equal(t, StatusOK, record.Status, record.Error)
	require.NotNil(t, record.PostgresMetrics)
	require.NotNil(t, record.TraversalTelemetry)
	require.NoError(t, record.TraversalTelemetry.Validate())

	summary := record.TraversalTelemetry.Summary
	require.Equal(t, string(optimize.ExpansionSearchPolicySuffixReverseGuardV1), summary.EmittedIdentity)
	require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), summary.RuntimeIdentity)
	require.Equal(t, "suffix_seeded_reverse", summary.RuntimeBranch)
	require.False(t, *summary.Overflow)
	require.False(t, *summary.FallbackExecuted)
	require.Equal(t, optimize.ExpansionSearchSuffixReverseGuardSuffixRowLimit, summary.Caps["suffix_rows"])
	require.Equal(t, optimize.ExpansionSearchSuffixReverseGuardStateLimit, summary.Caps["state_rows"])

	diagnostic := record.TraversalTelemetry.Diagnostic
	require.Equal(t, TraversalTelemetryCounterStatusComplete, diagnostic.CounterStatus)
	require.NotNil(t, diagnostic.PlanReplay)
	counters := diagnostic.PlanReplay.Counters
	t.Logf("suffix rows=%d state rows=%d output rows=%d", counters["suffix_guard_suffix_rows"], counters["suffix_guard_state_rows"], counters["suffix_guard_output_rows"])
	require.Equal(t, int64(1), counters["suffix_guard_candidate_marker_rows"])
	require.Zero(t, counters["suffix_guard_fallback_marker_rows"])
	require.Equal(t, int64(1), counters["suffix_guard_candidate_executor_loops"])
	require.Zero(t, counters["suffix_guard_fallback_executor_loops"])
	require.Zero(t, counters["suffix_guard_fallback_branch_rows"])

	// The counter derivation is accepted only when each materialized branch
	// body has exactly one marker outer child and one executor inner child.
	for _, branch := range []string{"candidate", "fallback"} {
		bodySuffix := "suffix_guard_" + branch + "_body"
		markerSuffix := "suffix_guard_" + branch + "_marker"
		var bodies []PostgresPlanNodeMetric
		for _, node := range record.PostgresMetrics.PlanNodes {
			if namedCTEBody(node, bodySuffix) {
				bodies = append(bodies, node)
			}
		}
		require.Len(t, bodies, 1, branch)
		var outer, inner int
		for _, node := range record.PostgresMetrics.PlanNodes {
			if node.ParentPlanNodeID != bodies[0].PlanNodeID {
				continue
			}
			if strings.EqualFold(node.ParentRelationship, "Outer") && strings.EqualFold(node.NodeType, "CTE Scan") &&
				strings.HasSuffix(strings.ToLower(node.CTEName), markerSuffix) {
				outer++
			}
			if strings.EqualFold(node.ParentRelationship, "Inner") {
				inner++
			}
		}
		require.Equal(t, 1, outer, branch)
		require.Equal(t, 1, inner, branch)
	}
}

// TestPostgreSQLSuffixReverseRetryPreservesExactRowsAndReceipts exercises the
// reverse-complete, state-overflow, no-path, and output-byte retry paths on
// open P1 training cases. It never selects a protected holdout.
func TestPostgreSQLSuffixReverseRetryPreservesExactRowsAndReceipts(t *testing.T) {
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
	for _, test := range []struct {
		name               string
		caseName           string
		stateLimit         int64
		expectedFirstEvent string
		expectedFallback   bool
		expectedFinalEvent string
	}{
		{
			name: "reverse complete", caseName: "GFSE-V3-TRAIN-Q4-C1-S1-productive_cycle_self_loop_path",
			expectedFirstEvent: "reverse_complete", expectedFinalEvent: "reverse_complete",
		},
		{
			name: "forced state retry", caseName: "GFSE-V3-TRAIN-Q4-C1-S1-productive_cycle_self_loop_path", stateLimit: 1,
			expectedFirstEvent: "forward_retry_state_overflow", expectedFallback: true, expectedFinalEvent: "exact_forward_retry_complete",
		},
		{
			name: "natural high reverse fan-in retry", caseName: "GFSE-P1-TRAIN-D09-F017-R0-X2-I1024-M1-Q1-high_reverse_fanin_path",
			expectedFirstEvent: "forward_retry_state_overflow", expectedFallback: true, expectedFinalEvent: "exact_forward_retry_complete",
		},
		{
			name: "no path exhaustion", caseName: "GFSE-P1-TRAIN-D09-F513-R0-X512-no_path_exhaustion",
			expectedFirstEvent: "reverse_complete", expectedFinalEvent: "reverse_complete",
		},
		{
			name: "output byte retry", caseName: "GFSE-P1-TRAIN-D00-F001-R0-X0-M4-P2100000-output_byte_retry_path",
			expectedFirstEvent: "forward_retry_output_bytes", expectedFallback: true, expectedFinalEvent: "exact_forward_retry_complete",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			selected, _, err := selectScaleCorpus(corpus, CorpusSelectors{Cases: []string{test.caseName}})
			require.NoError(t, err)
			require.Len(t, selected.Cases, 1)
			require.Equal(t, "training", selected.Cases[0].Shape.QualificationSplit)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, selected, 1, 1, nil, false, nil, "", "")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
			runner.repeatableRead = true
			runner.traversalTelemetry = postgresTraversalTelemetryDiagnostic
			runner.toolOptions.EnableExpansionSuffixReverseRetry = true
			runner.toolOptions.SuffixReverseGuardStateLimit = test.stateLimit

			records, err := runner.Run(ctx, 0, 2, selected)
			require.NoError(t, err)
			require.Len(t, records, 1)
			record := records[0]
			require.Equal(t, StatusOK, record.Status, record.Error)
			require.Contains(t, record.SQL, "dawgs.suffix_reverse_retry_status")
			require.NotContains(t, record.SQL, "_suffix_guard_fallback_body")
			for _, sample := range record.Stats.Samples {
				if sample.RuntimeAttestation != "timed_invocation" {
					continue
				}
				require.NotEmpty(t, sample.RuntimeReceiptEvents)
				firstEvent := sample.RuntimeReceiptEvents[0]
				require.Equal(t, test.expectedFirstEvent, firstEvent.RuntimeBranch)
				finalEvent := sample.RuntimeReceiptEvents[len(sample.RuntimeReceiptEvents)-1]
				require.Equal(t, test.expectedFinalEvent, finalEvent.RuntimeBranch)
				require.NotNil(t, sample.FallbackExecuted)
				require.Equal(t, test.expectedFallback, *sample.FallbackExecuted)
			}
		})
	}
}
