// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

func TestResourceGateAllowsCompactSessionWorkspaceButRejectsExecutorSpill(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	record := CaseResult{
		Dataset: "fixture", Name: "case", ExecutionMode: ModePostgresSQL, Status: StatusOK,
		Shape:           WorkloadShape{FixtureTier: "normal"},
		Optimization:    &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{Family: "SP", Applied: "SP-S4-C-D"}}},
		PostgresMetrics: &PostgresPlanMetrics{Buffers: Buffers{LocalWritten: 1}},
	}
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	passed, err := createResourceGateReport(artifact, filepath.Join(t.TempDir(), "report.json"))
	require.NoError(t, err)
	require.True(t, passed)

	record.PostgresMetrics.Buffers.TempWritten = 1
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	passed, err = createResourceGateReport(artifact, filepath.Join(t.TempDir(), "spill-report.json"))
	require.NoError(t, err)
	require.False(t, passed)
}

func TestResourceGateRecognizesASPProductionArchitecture(t *testing.T) {
	record := CaseResult{Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{Family: "ASP", Applied: "ASP-A1-DAG"}}}}
	require.Equal(t, "ASP-A1-DAG", appliedShortestArchitecture(record))
}

func TestResourceGateChecksFullComparatorReferenceResources(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	record := CaseResult{
		Dataset: "fixture", Name: "case", ExecutionMode: ModePostgresSQL, Status: StatusOK,
		Shape: WorkloadShape{FixtureTier: "normal"},
		PostgresReferences: []PostgresReferenceResult{{
			Name: "s4", Architecture: "SP-S4-C-D", FullComparator: true,
			PostgresMetrics: &PostgresPlanMetrics{Buffers: Buffers{TempWritten: 1}},
		}},
	}
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	output := filepath.Join(t.TempDir(), "report.json")
	passed, err := createResourceGateReport(artifact, output)
	require.NoError(t, err)
	require.False(t, passed)

	var report ResourceGateReport
	raw, err := os.ReadFile(output)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &report))
	require.Len(t, report.Cases, 2)
	require.Equal(t, "s4", report.Cases[1].Reference)
	require.Contains(t, report.Cases[1].Reasons, "portable candidate used temporary buffers")
}

func TestResourceGateAttributesDirectPreflightIncumbentFallback(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	records := []CaseResult{
		{
			Dataset: "fixture", Name: "fallback", ExecutionMode: ModePostgresSQL, Status: StatusOK,
			Shape:            WorkloadShape{FixtureTier: "normal"},
			Optimization:     &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{Family: "SP", Applied: "SP-S0-DIRECT"}}},
			PostgresMetrics:  &PostgresPlanMetrics{Buffers: Buffers{LocalWritten: 1}},
			PostgresPlanJSON: json.RawMessage(`[{"Plan":{"Plans":[{"Alias":"bidirectional_sp_harness","Actual Loops":1}]}}]`),
		},
		{
			Dataset: "fixture", Name: "direct", ExecutionMode: ModePostgresSQL, Status: StatusOK,
			Shape:            WorkloadShape{FixtureTier: "normal"},
			Optimization:     &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{Family: "SP", Applied: "SP-S0-DIRECT"}}},
			PostgresPlanJSON: json.RawMessage(`[{"Plan":{"Plans":[{"Function Name":"bidirectional_sp_harness","Actual Loops":0}]}}]`),
		},
	}
	require.NoError(t, writeJSONLFile(artifact, records))
	output := filepath.Join(t.TempDir(), "report.json")
	passed, err := createResourceGateReport(artifact, output)
	require.NoError(t, err)
	require.True(t, passed)

	var report ResourceGateReport
	raw, err := os.ReadFile(output)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &report))
	require.Equal(t, "SP-S0", report.Cases[1].FallbackArchitecture)
}

func TestResourceGateRejectsDirectPreflightWorkspaceOnDirectHit(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	record := CaseResult{
		Dataset: "fixture", Name: "direct", ExecutionMode: ModePostgresSQL, Status: StatusOK,
		Shape:            WorkloadShape{FixtureTier: "normal"},
		Optimization:     &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{Family: "SP", Applied: "SP-S0-DIRECT"}}},
		PostgresMetrics:  &PostgresPlanMetrics{Buffers: Buffers{LocalWritten: 1}},
		PostgresPlanJSON: json.RawMessage(`[{"Plan":{"Plans":[{"Alias":"bidirectional_sp_harness","Actual Loops":0}]}}]`),
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
