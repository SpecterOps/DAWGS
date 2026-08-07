// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

func TestResourceGateRejectsNormalPortableCandidateSpill(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	record := CaseResult{
		Dataset: "fixture", Name: "case", ExecutionMode: ModePostgresSQL, Status: StatusOK,
		Shape:           WorkloadShape{FixtureTier: "normal"},
		Optimization:    &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{Family: "SP", Applied: "SP-S4-C-D"}}},
		PostgresMetrics: &PostgresPlanMetrics{Buffers: Buffers{TempWritten: 1}},
	}
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	passed, err := createResourceGateReport(artifact, filepath.Join(t.TempDir(), "report.json"))
	require.NoError(t, err)
	require.False(t, passed)
}

func TestResourceGateAllowsStressDiagnosticsAndExactFallback(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	records := []CaseResult{
		{Dataset: "fixture", Name: "stress", ExecutionMode: ModePostgresSQL, Status: StatusOK, Shape: WorkloadShape{FixtureTier: "stress"}, PostgresMetrics: &PostgresPlanMetrics{Buffers: Buffers{TempWritten: 1}}},
		{Dataset: "fixture", Name: "fallback", ExecutionMode: ModePostgresSQL, Status: StatusOK, Shape: WorkloadShape{FixtureTier: "normal"}, Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{Family: "SP", Selected: "SP-S0"}}}, PostgresMetrics: &PostgresPlanMetrics{Buffers: Buffers{LocalWritten: 1}}},
	}
	require.NoError(t, writeJSONLFile(artifact, records))
	passed, err := createResourceGateReport(artifact, filepath.Join(t.TempDir(), "report.json"))
	require.NoError(t, err)
	require.True(t, passed)
}
