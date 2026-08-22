// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBackendDeltaReportIsDescriptiveAndRequiresMatchedObservations verifies that equal stable rows make backend timings comparable while the report remains explicitly non-gating.
func TestBackendDeltaReportIsDescriptiveAndRequiresMatchedObservations(t *testing.T) {
	root := t.TempDir()
	artifact, output := filepath.Join(root, "records.jsonl"), filepath.Join(root, "delta.json")
	records := []CaseResult{
		{
			Dataset:           "fixture",
			Name:              "case",
			ExecutionMode:     ModePostgresSQL,
			Status:            StatusOK,
			RowCount:          1,
			StableObservation: true,
			ObservedRows:      []string{"one"},
			Stats: DurationStats{
				Median: time.Millisecond,
				P95:    2 * time.Millisecond,
			},
		},
		{
			Dataset:           "fixture",
			Name:              "case",
			ExecutionMode:     ModeNeo4j,
			Status:            StatusOK,
			RowCount:          1,
			StableObservation: true,
			ObservedRows:      []string{"one"},
			Stats: DurationStats{
				Median: 2 * time.Millisecond,
				P95:    3 * time.Millisecond,
			},
		},
	}
	require.NoError(t, writeJSONLFile(artifact, records))
	require.NoError(t, createBackendDeltaReport(artifact, output))
	raw, err := os.ReadFile(output)
	require.NoError(t, err)
	var report BackendDeltaReport
	require.NoError(t, json.Unmarshal(raw, &report))
	require.Len(t, report.Cases, 1)
	require.True(t, report.Cases[0].ObservationsComparable)
	require.True(t, report.Cases[0].ObservationsMatch)
	require.Equal(t, 2.0, report.Cases[0].MedianNeo4jOverPG)
	require.Contains(t, report.Notice, "Descriptive only")
}

// TestBackendDeltaReportComparesPersistedObservations verifies that differing canonical row payloads are reported as a semantic mismatch even when row counts agree.
func TestBackendDeltaReportComparesPersistedObservations(t *testing.T) {
	root := t.TempDir()
	artifact, output := filepath.Join(root, "records.jsonl"), filepath.Join(root, "delta.json")
	records := []CaseResult{
		{
			Dataset:           "fixture",
			Name:              "case",
			ExecutionMode:     ModePostgresSQL,
			Status:            StatusOK,
			RowCount:          1,
			StableObservation: true,
			ObservedRows:      []string{"postgres"},
		},
		{
			Dataset:           "fixture",
			Name:              "case",
			ExecutionMode:     ModeNeo4j,
			Status:            StatusOK,
			RowCount:          1,
			StableObservation: true,
			ObservedRows:      []string{"neo4j"},
		},
	}
	require.NoError(t, writeJSONLFile(artifact, records))
	require.NoError(t, createBackendDeltaReport(artifact, output))
	raw, err := os.ReadFile(output)
	require.NoError(t, err)
	var report BackendDeltaReport
	require.NoError(t, json.Unmarshal(raw, &report))
	require.Len(t, report.Cases, 1)
	require.False(t, report.Cases[0].ObservationsMatch)
}

// TestBackendDeltaReportDoesNotTreatAbsentObservationsAsMatching verifies that matching cardinalities cannot establish comparability without persisted stable row observations.
func TestBackendDeltaReportDoesNotTreatAbsentObservationsAsMatching(t *testing.T) {
	root := t.TempDir()
	artifact, output := filepath.Join(root, "records.jsonl"), filepath.Join(root, "delta.json")
	records := []CaseResult{
		{
			Dataset:       "fixture",
			Name:          "case",
			ExecutionMode: ModePostgresSQL,
			Status:        StatusOK,
			RowCount:      1,
		},
		{
			Dataset:       "fixture",
			Name:          "case",
			ExecutionMode: ModeNeo4j,
			Status:        StatusOK,
			RowCount:      1,
		},
	}
	require.NoError(t, writeJSONLFile(artifact, records))
	require.NoError(t, createBackendDeltaReport(artifact, output))
	raw, err := os.ReadFile(output)
	require.NoError(t, err)
	var report BackendDeltaReport
	require.NoError(t, json.Unmarshal(raw, &report))
	require.False(t, report.Cases[0].ObservationsComparable)
	require.False(t, report.Cases[0].ObservationsMatch)
}

// TestBackendDeltaReportPreservesRepeatedRounds verifies that matched backend observations remain separate, ordered report cases for each measurement round.
func TestBackendDeltaReportPreservesRepeatedRounds(t *testing.T) {
	root := t.TempDir()
	artifact, output := filepath.Join(root, "records.jsonl"), filepath.Join(root, "delta.json")
	var records []CaseResult
	for round := 1; round <= 2; round++ {
		for _, mode := range []ExecutionMode{ModePostgresSQL, ModeNeo4j} {
			records = append(records, CaseResult{
				Dataset:           "fixture",
				Name:              "case",
				ExecutionMode:     mode,
				Status:            StatusOK,
				StableObservation: true,
				ObservedRows:      []string{"one"},
				RowCount:          1,
				Environment:       &RunEnvironment{Round: round},
				Stats:             DurationStats{Median: time.Duration(round) * time.Millisecond},
			})
		}
	}
	require.NoError(t, writeJSONLFile(artifact, records))
	require.NoError(t, createBackendDeltaReport(artifact, output))
	raw, err := os.ReadFile(output)
	require.NoError(t, err)
	var report BackendDeltaReport
	require.NoError(t, json.Unmarshal(raw, &report))
	require.Len(t, report.Cases, 2)
	require.Equal(t, 1, report.Cases[0].Round)
	require.Equal(t, 2, report.Cases[1].Round)
}

// TestBackendDeltaReportPreservesIncompletePairs verifies a missing backend
// remains visible instead of disappearing from an intersection-only report.
func TestBackendDeltaReportPreservesIncompletePairs(t *testing.T) {
	root := t.TempDir()
	artifact, output := filepath.Join(root, "records.jsonl"), filepath.Join(root, "delta.json")
	records := []CaseResult{{
		Dataset:       "fixture",
		Name:          "postgres-only",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
	}}
	require.NoError(t, writeJSONLFile(artifact, records))
	require.NoError(t, createBackendDeltaReport(artifact, output))
	raw, err := os.ReadFile(output)
	require.NoError(t, err)
	var report BackendDeltaReport
	require.NoError(t, json.Unmarshal(raw, &report))
	require.Equal(t, 2, report.Version)
	require.Len(t, report.Cases, 1)
	require.False(t, report.Cases[0].Complete)
	require.Equal(t, "missing_neo4j", report.Cases[0].IncompleteReason)
	require.Zero(t, report.Cases[0].MedianNeo4jOverPG)
	require.Zero(t, report.Cases[0].P95Neo4jOverPG)
}

// TestBackendDeltaReportRanksRepeatedRoundOutliers verifies the descriptive
// report turns matched rounds into a runtime-attributed optimization ledger.
func TestBackendDeltaReportRanksRepeatedRoundOutliers(t *testing.T) {
	root := t.TempDir()
	artifact, output := filepath.Join(root, "records.jsonl"), filepath.Join(root, "delta.json")
	var records []CaseResult
	for round, postgresMedian := range []time.Duration{8 * time.Millisecond, 12 * time.Millisecond} {
		environment := &RunEnvironment{Round: round + 1}
		records = append(records,
			CaseResult{
				Dataset:           "fixture",
				Name:              "slow",
				Category:          "shortest_path",
				Shape:             WorkloadShape{Direction: "inbound", ExpectedStateClass: "hidden_fanin"},
				ExecutionMode:     ModePostgresSQL,
				Status:            StatusOK,
				RowCount:          1,
				StableObservation: true,
				ObservedRows:      []string{"one"},
				Environment:       environment,
				Stats:             DurationStats{Median: postgresMedian, P95: postgresMedian + time.Millisecond},
				SQLFingerprint:    "sql-fingerprint",
				FallbackReason:    "tournament_unqualified",
				TraversalTelemetry: &TraversalExecutionTelemetry{Summary: TraversalExecutionSummary{
					RuntimeIdentity: "SP-S4-C-D",
					AppliedIdentity: "SP-S4-C-D",
					RuntimeBranch:   "compact_distance",
					ObservationMode: "distance",
					SelectorVersion: "sp-static-v5-contained",
				}},
			},
			CaseResult{
				Dataset:           "fixture",
				Name:              "slow",
				Category:          "shortest_path",
				ExecutionMode:     ModeNeo4j,
				Status:            StatusOK,
				RowCount:          1,
				StableObservation: true,
				ObservedRows:      []string{"one"},
				Environment:       environment,
				Stats:             DurationStats{Median: 2 * time.Millisecond, P95: 3 * time.Millisecond},
			},
		)
	}
	// A PostgreSQL win remains in the complete case report but not the outlier ledger.
	records = append(records,
		CaseResult{Dataset: "fixture", Name: "fast", ExecutionMode: ModePostgresSQL, Status: StatusOK, RowCount: 1, StableObservation: true, ObservedRows: []string{"one"}, Stats: DurationStats{Median: time.Millisecond}},
		CaseResult{Dataset: "fixture", Name: "fast", ExecutionMode: ModeNeo4j, Status: StatusOK, RowCount: 1, StableObservation: true, ObservedRows: []string{"one"}, Stats: DurationStats{Median: 2 * time.Millisecond}},
	)

	require.NoError(t, writeJSONLFile(artifact, records))
	require.NoError(t, createBackendDeltaReport(artifact, output))
	raw, err := os.ReadFile(output)
	require.NoError(t, err)
	var report BackendDeltaReport
	require.NoError(t, json.Unmarshal(raw, &report))
	require.Len(t, report.Outliers, 1)
	outlier := report.Outliers[0]
	require.Equal(t, "slow", outlier.Name)
	require.Equal(t, 2, outlier.Rounds)
	require.Equal(t, 4.0, outlier.MedianPostgresOverNeo4j)
	require.Equal(t, 8*time.Millisecond, outlier.PostgresMedian)
	require.Equal(t, 2*time.Millisecond, outlier.Neo4jMedian)
	require.Equal(t, []string{"SP-S4-C-D"}, outlier.AppliedIdentities)
	require.Equal(t, []string{"compact_distance"}, outlier.RuntimeBranches)
	require.Equal(t, []string{"tournament_unqualified"}, outlier.FallbackReasons)
	require.Equal(t, "hidden_fanin", outlier.ExpectedStateClass)
}
