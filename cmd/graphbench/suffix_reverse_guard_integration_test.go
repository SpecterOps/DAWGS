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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// TestPostgreSQLSuffixRouteComponentPreservesExactRowsAndReceipt exercises the
// default-off direct component arm on an open training fixture. It verifies
// the component is one reverse statement with no guard, retry, or incumbent
// arm; it is not a performance qualification.
func TestPostgreSQLSuffixRouteComponentPreservesExactRowsAndReceipt(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, selected, 1, 1, nil, false, nil, "", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
	runner.repeatableRead = true
	runner.traversalTelemetry = postgresTraversalTelemetryDiagnostic
	runner.toolOptions.EnableExpansionSuffixRouteComponent = true

	records, err := runner.Run(ctx, 0, 1, selected)
	require.NoError(t, err)
	require.Len(t, records, 1)
	record := records[0]
	require.Equal(t, StatusOK, record.Status, record.Error)
	require.NotNil(t, record.TraversalTelemetry)
	require.NoError(t, record.TraversalTelemetry.Validate())
	require.NotNil(t, record.TraversalTelemetry.Diagnostic)
	require.Equal(t, TraversalTelemetryCounterStatusComplete, record.TraversalTelemetry.Diagnostic.CounterStatus)
	require.NotNil(t, record.TraversalTelemetry.Diagnostic.Counters.SuffixComponent)
	require.Equal(t, int64(1), *record.TraversalTelemetry.Diagnostic.Counters.SuffixComponent.ReceiptRows)
	require.Contains(t, record.SQL, "_suffix_seeded_component_receipt")
	require.NotContains(t, record.SQL, "_suffix_guard_")
	require.NotContains(t, record.SQL, "EXPANSION-STEPWISE-FORWARD")

	summary := record.TraversalTelemetry.Summary
	require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), summary.EmittedIdentity)
	require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), summary.RuntimeIdentity)
	require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), summary.AppliedIdentity)
	require.Equal(t, optimize.ExpansionSearchSelectorSuffixRouteComponentV1, summary.SelectorVersion)
	require.Equal(t, "selected", summary.RuntimeBranch)
	require.False(t, *summary.FallbackExecuted)

	for _, sample := range record.Stats.Samples {
		if sample.RuntimeAttestation != "timed_invocation" {
			continue
		}
		require.Len(t, sample.RuntimeReceiptEvents, 1)
		require.Equal(t, "suffix_route_component", sample.RuntimeReceiptEvents[0].RuntimeBranch)
		require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), sample.RuntimeReceiptEvents[0].RuntimeIdentity)
	}
}

// TestPostgreSQLSuffixRouteComponentClosureRecordsPreparedStateAndWorkspace
// verifies the closure records the first fresh miss, reusable prepared hits,
// same-backend pool reacquisition, and complete component workspace evidence
// without enabling a selector or retry.
func TestPostgreSQLSuffixRouteComponentClosureRecordsPreparedStateAndWorkspace(t *testing.T) {
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
	selected, _, err := selectScaleCorpus(corpus, CorpusSelectors{
		Cases: []string{"GFSE-SRC-V1-TARGET-D16-F1024-sparse_endpoint_ids"},
	})
	require.NoError(t, err)
	require.Len(t, selected.Cases, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, selected, 1, 1, nil, false, nil, "", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
	runner.repeatableRead = true
	runner.traversalTelemetry = postgresTraversalTelemetryDiagnostic
	runner.toolOptions.EnableExpansionSuffixRouteComponent = true
	runner.suffixRouteComponentClosure = true
	runner.sessionMemoryCeilingBytes = 1 << 20
	runner.poolMemoryCeilingBytes = 1 << 20

	records, err := runner.Run(ctx, 0, 1, selected)
	require.NoError(t, err)
	require.Len(t, records, 1)
	record := records[0]
	require.Equal(t, StatusOK, record.Status, record.Error)
	require.NotNil(t, record.ClientWaterfall)
	require.Len(t, record.ClientWaterfall.Samples, 1)
	require.NotNil(t, record.PostgresBoundaryClosure)
	closure := record.PostgresBoundaryClosure
	require.NotEmpty(t, closure.SQLFingerprint)
	expectedObservation, err := stableObservationSHA256(record.ObservedRows)
	require.NoError(t, err)
	require.Len(t, closure.SameSessionPreparedHits, 1)
	require.Len(t, closure.PoolReacquiredPreparedHits, 1)
	require.Equal(t, closure.PoolPreparedMiss.ConnectionID, closure.PoolReacquiredPreparedHits[0].ConnectionID)
	for _, sample := range postgresBoundaryClosureSamples(*closure) {
		require.Equal(t, record.RowCount, sample.Rows)
		require.NotNil(t, sample.WorkspaceBytes)
		require.NotEmpty(t, sample.ConnectionID)
		require.Equal(t, expectedObservation, sample.ObservationSHA256)
	}
	require.LessOrEqual(t, closure.Workspace.SessionPeakBytes, runner.sessionMemoryCeilingBytes)
	require.Equal(t, closure.Workspace.SessionPeakBytes, closure.Workspace.PoolPeakBytes)
	require.LessOrEqual(t, closure.Workspace.PerQueryPeakBytes, runner.poolMemoryCeilingBytes)
	require.Zero(t, closure.Workspace.PerQueryPeakBytes)
	require.Zero(t, closure.Workspace.FreshSessionPeakBytes)
	require.Zero(t, closure.Workspace.SessionPeakBytes)
	require.Zero(t, closure.Workspace.PoolPeakBytes)
	require.NotNil(t, record.TraversalTelemetry)
	require.NoError(t, record.TraversalTelemetry.Validate())
	require.Contains(t, record.TraversalTelemetry.Diagnostic.RequiredFamilies, TraversalTelemetryFamilyWorkspace)
	require.NotNil(t, record.TraversalTelemetry.Diagnostic.Counters.Workspace)
	require.Equal(t, closure.Workspace.SessionPeakBytes, *record.TraversalTelemetry.Diagnostic.Counters.Workspace.SessionPeakBytes)
}

// TestPostgreSQLSuffixRouteComponentRecordsNoPathReceipt verifies the direct
// component records execution even when its exact reverse query returns no
// public rows. This keeps no-path component measurements fail-closed.
func TestPostgreSQLSuffixRouteComponentRecordsNoPathReceipt(t *testing.T) {
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
	selected, _, err := selectScaleCorpus(corpus, CorpusSelectors{Cases: []string{"GFSE-P1-TRAIN-D09-F513-R0-X512-no_path_exhaustion"}})
	require.NoError(t, err)
	require.Len(t, selected.Cases, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, selected, 1, 1, nil, false, nil, "", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
	runner.repeatableRead = true
	runner.traversalTelemetry = postgresTraversalTelemetryDiagnostic
	runner.toolOptions.EnableExpansionSuffixRouteComponent = true

	records, err := runner.Run(ctx, 0, 1, selected)
	require.NoError(t, err)
	require.Len(t, records, 1)
	record := records[0]
	require.Equal(t, StatusOK, record.Status, record.Error)
	for _, sample := range record.Stats.Samples {
		if sample.RuntimeAttestation != "timed_invocation" {
			continue
		}
		require.Len(t, sample.RuntimeReceiptEvents, 1)
		require.Equal(t, "suffix_route_component", sample.RuntimeReceiptEvents[0].RuntimeBranch)
		require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), sample.RuntimeReceiptEvents[0].RuntimeIdentity)
	}
}

// TestPostgreSQLSuffixRouteComponentCancellationReusesPoolSession proves the
// direct component handles PostgreSQL cancellation, rolls the failed
// transaction back, returns its single connection to the pool, and remains
// usable from the reacquired physical backend. It is operational evidence,
// not a timing qualification.
func TestPostgreSQLSuffixRouteComponentCancellationReusesPoolSession(t *testing.T) {
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
	selected, _, err := selectScaleCorpus(corpus, CorpusSelectors{
		Cases: []string{"GFSE-SRC-V1-TARGET-D17-F1025-sparse_path"},
	})
	require.NoError(t, err)
	require.Len(t, selected.Cases, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, selected, 1, 1, nil, false, nil, "", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
	runner.repeatableRead = true
	runner.traversalTelemetry = postgresTraversalTelemetryDiagnostic
	runner.toolOptions.EnableExpansionSuffixRouteComponent = true

	records, err := runner.Run(ctx, 0, 1, selected)
	require.NoError(t, err)
	require.Len(t, records, 1)
	record := records[0]
	require.Equal(t, StatusOK, record.Status, record.Error)
	require.NotNil(t, record.TraversalTelemetry)
	require.NoError(t, record.TraversalTelemetry.Validate())
	require.NotNil(t, record.TraversalTelemetry.Diagnostic.Counters.SuffixComponent)
	require.Equal(t, TraversalTelemetryCounterStatusComplete, record.TraversalTelemetry.Diagnostic.CounterStatus)

	translation, sqlQuery, err := runner.translateCypher(ctx, selected.Cases[0].Cypher, record.Params)
	require.NoError(t, err)
	require.Contains(t, sqlQuery, "_suffix_seeded_component_receipt")
	require.NotContains(t, sqlQuery, "EXPANSION-STEPWISE-FORWARD")
	queryArgs := []any{pgx.QueryExecModeCacheStatement, pgx.QueryResultFormats{pgx.BinaryFormatCode}, pgx.NamedArgs(translation.Parameters)}

	connectionHandle, err := runner.pool.Acquire(ctx)
	require.NoError(t, err)
	backendPID := connectionHandle.Conn().PgConn().PID()
	tx, err := connectionHandle.BeginTx(ctx, postgresConcurrencyTxOptions())
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "set local statement_timeout = '1ms'")
	require.NoError(t, err)
	started := time.Now()
	rows, queryErr := tx.Query(ctx, sqlQuery, queryArgs...)
	if queryErr == nil {
		for rows.Next() {
			_, queryErr = rows.Values()
			if queryErr != nil {
				break
			}
		}
		rows.Close()
		if queryErr == nil {
			queryErr = rows.Err()
		}
	}
	cancellationLatency := time.Since(started)
	var postgresError *pgconn.PgError
	require.ErrorAs(t, queryErr, &postgresError)
	require.Equal(t, "57014", postgresError.Code)
	require.Less(t, cancellationLatency, 250*time.Millisecond)
	require.NoError(t, tx.Rollback(ctx))
	connectionHandle.Release()

	reusedHandle, err := runner.pool.Acquire(ctx)
	require.NoError(t, err)
	defer reusedHandle.Release()
	var reusedPID uint32
	require.NoError(t, reusedHandle.QueryRow(ctx, "select pg_backend_pid()").Scan(&reusedPID))
	require.Equal(t, backendPID, reusedPID)

	rows, err = reusedHandle.Query(ctx, sqlQuery, queryArgs...)
	require.NoError(t, err)
	rowCount := int64(0)
	for rows.Next() {
		_, err = rows.Values()
		require.NoError(t, err)
		rowCount++
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Equal(t, record.RowCount, rowCount)
	t.Logf("cancelled direct suffix-route component in %s; pool reused backend PID %d", cancellationLatency, backendPID)
}
