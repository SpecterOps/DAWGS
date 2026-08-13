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
