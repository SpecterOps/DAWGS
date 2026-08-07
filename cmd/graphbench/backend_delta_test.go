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

func TestBackendDeltaReportIsDescriptiveAndRequiresMatchedObservations(t *testing.T) {
	root := t.TempDir()
	artifact, output := filepath.Join(root, "records.jsonl"), filepath.Join(root, "delta.json")
	records := []CaseResult{
		{Dataset: "fixture", Name: "case", ExecutionMode: ModePostgresSQL, Status: StatusOK, RowCount: 1, StableObservation: true, ObservedRows: []string{"one"}, Stats: DurationStats{Median: time.Millisecond, P95: 2 * time.Millisecond}},
		{Dataset: "fixture", Name: "case", ExecutionMode: ModeNeo4j, Status: StatusOK, RowCount: 1, StableObservation: true, ObservedRows: []string{"one"}, Stats: DurationStats{Median: 2 * time.Millisecond, P95: 3 * time.Millisecond}},
	}
	require.NoError(t, writeJSONLFile(artifact, records))
	require.NoError(t, createBackendDeltaReport(artifact, output))
	raw, err := os.ReadFile(output)
	require.NoError(t, err)
	var report BackendDeltaReport
	require.NoError(t, json.Unmarshal(raw, &report))
	require.Len(t, report.Cases, 1)
	require.True(t, report.Cases[0].ObservationsMatch)
	require.Equal(t, 2.0, report.Cases[0].MedianNeo4jOverPG)
	require.Contains(t, report.Notice, "Descriptive only")
}

func TestBackendDeltaReportComparesPersistedObservations(t *testing.T) {
	root := t.TempDir()
	artifact, output := filepath.Join(root, "records.jsonl"), filepath.Join(root, "delta.json")
	records := []CaseResult{
		{Dataset: "fixture", Name: "case", ExecutionMode: ModePostgresSQL, Status: StatusOK, RowCount: 1, StableObservation: true, ObservedRows: []string{"postgres"}},
		{Dataset: "fixture", Name: "case", ExecutionMode: ModeNeo4j, Status: StatusOK, RowCount: 1, StableObservation: true, ObservedRows: []string{"neo4j"}},
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
