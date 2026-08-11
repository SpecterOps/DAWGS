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

// TestResourceGateAllowsCompactSessionWorkspaceButRejectsExecutorSpill verifies that local workspace writes are permitted for the compact architecture while temporary-buffer spill fails the gate.
func TestResourceGateAllowsCompactSessionWorkspaceButRejectsExecutorSpill(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	record := CaseResult{
		Dataset:       "fixture",
		Name:          "case",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
		Shape: WorkloadShape{
			FixtureTier: "normal",
		},
		Optimization: &translate.OptimizationSummary{
			TargetOutcomes: []translate.TargetLoweringOutcome{{
				Family:  "SP",
				Applied: "SP-S4-C-D",
			}},
		},
		PostgresMetrics: &PostgresPlanMetrics{
			Buffers: Buffers{
				LocalWritten: 1,
			},
		},
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

// TestResourceGateRecognizesASPProductionArchitecture verifies that the applied all-shortest-path lowering, rather than a fallback label, identifies the production architecture.
func TestResourceGateRecognizesASPProductionArchitecture(t *testing.T) {
	record := CaseResult{
		Optimization: &translate.OptimizationSummary{
			TargetOutcomes: []translate.TargetLoweringOutcome{{
				Family:  "ASP",
				Applied: "ASP-A1-DAG",
			}},
		},
	}
	require.Equal(t, "ASP-A1-DAG", appliedPostgresArchitecture(record))
}

// TestResourceGateChecksFullComparatorReferenceResources verifies that temporary-buffer usage in a full comparator becomes its own failing report case with arm attribution.
func TestResourceGateChecksFullComparatorReferenceResources(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	record := CaseResult{
		Dataset:       "fixture",
		Name:          "case",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
		Shape: WorkloadShape{
			FixtureTier: "normal",
		},
		PostgresReferences: []PostgresReferenceResult{{
			Name:           "s4",
			Architecture:   "SP-S4-C-D",
			FullComparator: true,
			PostgresMetrics: &PostgresPlanMetrics{
				Buffers: Buffers{
					TempWritten: 1,
				},
			},
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

// TestResourceGateAttributesDirectPreflightIncumbentFallback verifies that a direct-preflight plan executing the recursive harness is attributed to SP-S0 fallback while a skipped harness remains direct.
func TestResourceGateAttributesDirectPreflightIncumbentFallback(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	records := []CaseResult{
		{
			Dataset:       "fixture",
			Name:          "fallback",
			ExecutionMode: ModePostgresSQL,
			Status:        StatusOK,
			Shape: WorkloadShape{
				FixtureTier: "normal",
			},
			Optimization: &translate.OptimizationSummary{
				TargetOutcomes: []translate.TargetLoweringOutcome{{
					Family:  "SP",
					Applied: "SP-S0-DIRECT",
				}},
			},
			PostgresMetrics: &PostgresPlanMetrics{
				Buffers: Buffers{
					LocalWritten: 1,
				},
			},
			PostgresPlanJSON: json.RawMessage(`[{"Plan":{"Plans":[{"Alias":"bidirectional_sp_harness","Actual Loops":1}]}}]`),
		},
		{
			Dataset:       "fixture",
			Name:          "direct",
			ExecutionMode: ModePostgresSQL,
			Status:        StatusOK,
			Shape: WorkloadShape{
				FixtureTier: "normal",
			},
			Optimization: &translate.OptimizationSummary{
				TargetOutcomes: []translate.TargetLoweringOutcome{{
					Family:  "SP",
					Applied: "SP-S0-DIRECT",
				}},
			},
			PostgresPlanJSON: json.RawMessage(`[{"Plan":{"Plans":[{"Function Name":"bidirectional_sp_harness","Actual Loops":0}]}}]`),
			PostgresMetrics:  &PostgresPlanMetrics{},
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

// TestResourceGateFailsClosedWithoutStructuredMetrics verifies that a successful portable candidate still fails resource gating when structured PostgreSQL metrics are absent.
func TestResourceGateFailsClosedWithoutStructuredMetrics(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	record := CaseResult{
		Dataset:       "fixture",
		Name:          "missing-metrics",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
		Shape:         WorkloadShape{FixtureTier: "normal"},
		Optimization: &translate.OptimizationSummary{
			TargetOutcomes: []translate.TargetLoweringOutcome{{Family: "SP", Applied: "SP-S4-C-D"}},
		},
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
	require.Contains(t, report.Cases[0].Reasons, "structured PostgreSQL plan metrics are missing")
}

// TestResourceGateRejectsDirectPreflightWorkspaceOnDirectHit verifies that a true direct hit cannot claim local workspace writes when the recursive harness executed zero times.
func TestResourceGateRejectsDirectPreflightWorkspaceOnDirectHit(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	record := CaseResult{
		Dataset:       "fixture",
		Name:          "direct",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
		Shape: WorkloadShape{
			FixtureTier: "normal",
		},
		Optimization: &translate.OptimizationSummary{
			TargetOutcomes: []translate.TargetLoweringOutcome{{
				Family:  "SP",
				Applied: "SP-S0-DIRECT",
			}},
		},
		PostgresMetrics: &PostgresPlanMetrics{
			Buffers: Buffers{
				LocalWritten: 1,
			},
		},
		PostgresPlanJSON: json.RawMessage(`[{"Plan":{"Plans":[{"Alias":"bidirectional_sp_harness","Actual Loops":0}]}}]`),
	}
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	passed, err := createResourceGateReport(artifact, filepath.Join(t.TempDir(), "report.json"))
	require.NoError(t, err)
	require.False(t, passed)
}

// TestResourceGateAllowsStressDiagnosticsAndExactFallback verifies that spill is diagnostic on stress fixtures and compact workspace use is allowed for an explicitly selected exact fallback.
func TestResourceGateAllowsStressDiagnosticsAndExactFallback(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	records := []CaseResult{
		{
			Dataset:       "fixture",
			Name:          "stress",
			ExecutionMode: ModePostgresSQL,
			Status:        StatusOK,
			Shape: WorkloadShape{
				FixtureTier: "stress",
			},
			PostgresMetrics: &PostgresPlanMetrics{
				Buffers: Buffers{
					TempWritten: 1,
				},
			},
		},
		{
			Dataset:       "fixture",
			Name:          "fallback",
			ExecutionMode: ModePostgresSQL,
			Status:        StatusOK,
			Shape: WorkloadShape{
				FixtureTier: "normal",
			},
			Optimization: &translate.OptimizationSummary{
				TargetOutcomes: []translate.TargetLoweringOutcome{{
					Family:   "SP",
					Selected: "SP-S0",
				}},
			},
			PostgresMetrics: &PostgresPlanMetrics{
				Buffers: Buffers{
					LocalWritten: 1,
				},
			},
		},
	}
	require.NoError(t, writeJSONLFile(artifact, records))
	passed, err := createResourceGateReport(artifact, filepath.Join(t.TempDir(), "report.json"))
	require.NoError(t, err)
	require.True(t, passed)
}
