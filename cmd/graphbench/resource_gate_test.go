// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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

// TestResourceGateRecognizesCompactBidirectionalWorkspaceArchitectures freezes
// local-workspace attribution for production and full-comparator B1/B2 arms.
func TestResourceGateRecognizesCompactBidirectionalWorkspaceArchitectures(t *testing.T) {
	for _, architecture := range []string{
		"SP-B1-C-ALT-NODE-D",
		"SP-B1-C-ALT-NODE-WE+MAT-M0",
		"SP-B2-C-MIN-LEVEL-D",
		"SP-B2-C-MIN-LEVEL-WE+MAT-M0",
	} {
		require.True(t, compactWorkspaceArchitecture(architecture), architecture)
		require.True(t, compactBidirectionalWorkspaceArchitecture(architecture), architecture)
	}
	require.True(t, compactWorkspaceArchitecture("SP-S4-C-D"))
	require.False(t, compactBidirectionalWorkspaceArchitecture("SP-S4-C-D"))
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

// TestResourceGateEnforcesTelemetryIdentityAndNumericSentinels verifies a
// candidate may observe exactly cap+1, while larger work or contradictory
// runtime attribution fails closed.
func TestResourceGateEnforcesTelemetryIdentityAndNumericSentinels(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	telemetry := validTraversalTelemetry()
	telemetry.Level = TraversalTelemetryLevelDiagnostic
	telemetry.Summary.RequestedIdentity = "SP-B1-C-ALT-NODE-D"
	telemetry.Summary.PlannedIdentities = []string{"SP-B1-C-ALT-NODE-D", "SP-S4-C-D"}
	telemetry.Summary.EmittedIdentity = "sp-bidirectional-tournament-v1"
	telemetry.Summary.RuntimeIdentity = "SP-B1-C-ALT-NODE-D"
	telemetry.Summary.AppliedIdentity = "SP-B1-C-ALT-NODE-D"
	telemetry.Summary.RuntimeOutcomeAvailable = telemetryBool(true)
	telemetry.Summary.Provenance["runtime_outcome_available"] = "executor.receipt"
	telemetry.Summary.Caps = map[string]int64{"state_rows": 32}
	telemetry.Summary.Provenance["caps.state_rows"] = "policy.state_cap"
	delete(telemetry.Summary.Provenance, "caps.state")
	telemetry.Diagnostic = ordinaryDiagnostic()
	telemetry.Diagnostic.Counters.Ordinary.PeakState = telemetryInt64(33)
	record := CaseResult{
		Dataset:            "fixture",
		Name:               "candidate",
		ExecutionMode:      ModePostgresSQL,
		Status:             StatusOK,
		Shape:              WorkloadShape{FixtureTier: "envelope", FallbackExpectation: "forbidden"},
		Environment:        &RunEnvironment{PoolSize: 1, SessionMemoryCeilingBytes: 1 << 20, PoolMemoryCeilingBytes: 1 << 20},
		TraversalTelemetry: &telemetry,
		PostgresMetrics:    &PostgresPlanMetrics{},
		Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
			Family: "SP", Applied: "SP-B1-C-ALT-NODE-D",
		}},
		},
	}
	record.TraversalTelemetry.Diagnostic.Counters.Workspace = &TraversalWorkspaceCounters{
		SessionPeakBytes: telemetryInt64(4096), PoolPeakBytes: telemetryInt64(4096),
	}
	record.TraversalTelemetry.Diagnostic.RequiredFamilies = append(
		record.TraversalTelemetry.Diagnostic.RequiredFamilies,
		TraversalTelemetryFamilyWorkspace,
	)
	record.TraversalTelemetry.Diagnostic.Provenance["workspace.session_peak_bytes"] = "test.session"
	record.TraversalTelemetry.Diagnostic.Provenance["workspace.pool_peak_bytes"] = "test.pool"
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	passingReportPath := filepath.Join(t.TempDir(), "cap-plus-one.json")
	passed, err := createResourceGateReport(artifact, passingReportPath)
	require.NoError(t, err)
	passingReportRaw, err := os.ReadFile(passingReportPath)
	require.NoError(t, err)
	var passingReport ResourceGateReport
	require.NoError(t, json.Unmarshal(passingReportRaw, &passingReport))
	require.True(t, passed, passingReport.Cases)

	telemetry.Diagnostic.Counters.Ordinary.PeakState = telemetryInt64(34)
	record.TraversalTelemetry = &telemetry
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	passed, err = createResourceGateReport(artifact, filepath.Join(t.TempDir(), "overflow.json"))
	require.NoError(t, err)
	require.False(t, passed)

	telemetry.Diagnostic.Counters.Ordinary.PeakState = telemetryInt64(32)
	telemetry.Summary.AppliedIdentity = "SP-S4-C-D"
	record.TraversalTelemetry = &telemetry
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	passed, err = createResourceGateReport(artifact, filepath.Join(t.TempDir(), "identity.json"))
	require.NoError(t, err)
	require.False(t, passed)
}

// TestResourceGateRequiresDiagnosticTelemetryForBidirectionalCandidates verifies
// opaque function work cannot qualify from outer EXPLAIN evidence alone.
func TestResourceGateRequiresDiagnosticTelemetryForBidirectionalCandidates(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	record := CaseResult{
		Dataset:         "fixture",
		Name:            "missing-telemetry",
		ExecutionMode:   ModePostgresSQL,
		Status:          StatusOK,
		Shape:           WorkloadShape{FixtureTier: "normal"},
		PostgresMetrics: &PostgresPlanMetrics{},
		Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
			Family: "SP", Applied: "SP-B2-C-MIN-LEVEL-D",
		}},
		},
	}
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	passed, err := createResourceGateReport(artifact, filepath.Join(t.TempDir(), "missing.json"))
	require.NoError(t, err)
	require.False(t, passed)

	telemetry := validTraversalTelemetry()
	telemetry.Level = TraversalTelemetryLevelDiagnostic
	telemetry.Summary.RequestedIdentity = "SP-B2-C-MIN-LEVEL-D"
	telemetry.Summary.PlannedIdentities = []string{"SP-B2-C-MIN-LEVEL-D", "SP-S4-C-D"}
	telemetry.Summary.EmittedIdentity = "sp-bidirectional-tournament-v1"
	telemetry.Summary.RuntimeIdentity = "SP-B2-C-MIN-LEVEL-D"
	telemetry.Summary.AppliedIdentity = "SP-B2-C-MIN-LEVEL-D"
	telemetry.Diagnostic = ordinaryDiagnostic()
	telemetry.Diagnostic.CounterStatus = TraversalTelemetryCounterStatusHiddenUnavailable
	telemetry.Diagnostic.IncompleteReasons = []string{"function scan hides invocation counters"}
	record.TraversalTelemetry = &telemetry
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	output := filepath.Join(t.TempDir(), "incomplete.json")
	passed, err = createResourceGateReport(artifact, output)
	require.NoError(t, err)
	require.False(t, passed)

	var report ResourceGateReport
	raw, err := os.ReadFile(output)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &report))
	require.Contains(t, report.Cases[0].Reasons, "candidate qualification requires complete executor counters; diagnostic status is hidden_counters_unavailable")
}

func TestResourceGateRejectsDeclaredMemoryCeilingsWithoutMeasuredWorkspace(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	record := CaseResult{
		Dataset: "fixture", Name: "declared-only", ExecutionMode: ModePostgresSQL, Status: StatusOK,
		Shape: WorkloadShape{FixtureTier: "normal"}, PostgresMetrics: &PostgresPlanMetrics{},
		Environment:  &RunEnvironment{PoolSize: 1, SessionMemoryCeilingBytes: 1024, PoolMemoryCeilingBytes: 4096},
		Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{Family: "SP", Applied: "SP-S4-C-D"}}},
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
	require.Contains(t, report.Cases[0].Reasons, "declared workspace memory ceilings lack measured session and pool high-water evidence")
}

func TestResourceGateRequiresCompleteOrientationPolicyAndExactBranchAttribution(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	telemetry := validTraversalTelemetry()
	telemetry.Level = TraversalTelemetryLevelDiagnostic
	telemetry.Summary.RequestedIdentity = "EXPANSION-SUFFIX-SEEDED-REVERSE"
	telemetry.Summary.PlannedIdentities = []string{"EXPANSION-SUFFIX-SEEDED-REVERSE", "EXPANSION-STEPWISE-FORWARD"}
	telemetry.Summary.EmittedIdentity = "orientation-probe-v1"
	telemetry.Summary.RuntimeIdentity = "EXPANSION-SUFFIX-SEEDED-REVERSE"
	telemetry.Summary.AppliedIdentity = "EXPANSION-SUFFIX-SEEDED-REVERSE"
	telemetry.Summary.SelectorVersion = "orientation-probe-v1"
	telemetry.Diagnostic = ordinaryDiagnostic()
	telemetry.Diagnostic.CounterStatus = TraversalTelemetryCounterStatusPlanPartial
	telemetry.Diagnostic.IncompleteReasons = []string{"plan evidence only"}
	telemetry.Diagnostic.PlanReplay = &TraversalPlanReplayEvidence{
		Source: "test", Counters: map[string]int64{"orientation_executed_candidate_rows": 1}, Flags: map[string]bool{},
		Provenance: map[string]string{"counters.orientation_executed_candidate_rows": "test.marker"},
	}
	record := CaseResult{
		Dataset: "fixture", Name: "orientation", ExecutionMode: ModePostgresSQL, Status: StatusOK,
		Shape: WorkloadShape{FixtureTier: "normal"}, PostgresMetrics: &PostgresPlanMetrics{}, TraversalTelemetry: &telemetry,
		Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
			Family: "fixed_suffix_expansion", Applied: "EXPANSION-SUFFIX-SEEDED-REVERSE", EmittedPolicy: "orientation-probe-v1",
		}}},
	}
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	passed, err := createResourceGateReport(artifact, filepath.Join(t.TempDir(), "report.json"))
	require.NoError(t, err)
	require.False(t, passed)
}

func TestResourceGateScopesStressFallbackToDeclaredExpectation(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	withoutExpectation := CaseResult{
		Dataset: "fixture", Name: "stress-no-overflow", ExecutionMode: ModePostgresSQL, Status: StatusOK,
		Shape: WorkloadShape{FixtureTier: "stress"}, PostgresMetrics: &PostgresPlanMetrics{},
		Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{Family: "SP", Applied: "SP-S0"}}},
	}
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{withoutExpectation}))
	passed, err := createResourceGateReport(artifact, filepath.Join(t.TempDir(), "no-expectation.json"))
	require.NoError(t, err)
	require.True(t, passed)

	withExpectation := withoutExpectation
	withExpectation.Name = "stress-overflow"
	withExpectation.Shape.FallbackExpectation = "required"
	telemetry := validTraversalTelemetry()
	telemetry.Summary.FallbackExecuted = telemetryBool(false)
	withExpectation.TraversalTelemetry = &telemetry
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{withExpectation}))
	passed, err = createResourceGateReport(artifact, filepath.Join(t.TempDir(), "expected.json"))
	require.NoError(t, err)
	require.False(t, passed)
}

func TestResourceGateValidatesExactOrientationMarkersAndProbeCounts(t *testing.T) {
	probeCounters := map[string]int64{
		"orientation_executed_candidate_rows":    1,
		"orientation_executed_incumbent_rows":    0,
		"orientation_root_probe_loops":           1,
		"orientation_suffix_probe_loops":         1,
		"orientation_boundary_probe_loops":       1,
		"orientation_forward_degree_probe_loops": 1,
		"orientation_reverse_degree_probe_loops": 1,
		"orientation_decision_loops":             1,
		"orientation_candidate_branch_loops":     1,
		"orientation_incumbent_branch_loops":     0,
	}
	diagnostic := &TraversalExecutionDiagnostic{PlanReplay: &TraversalPlanReplayEvidence{Counters: probeCounters}}
	gateCase := &ResourceGateCase{}
	appendOrientationAttributionReasons(gateCase, diagnostic)
	require.Empty(t, gateCase.Reasons)

	probeCounters["orientation_executed_incumbent_rows"] = 1
	probeCounters["orientation_root_probe_loops"] = 2
	delete(probeCounters, "orientation_suffix_probe_loops")
	appendOrientationAttributionReasons(gateCase, diagnostic)
	require.Contains(t, strings.Join(gateCase.Reasons, "\n"), "exactly one selected arm")
	require.Contains(t, strings.Join(gateCase.Reasons, "\n"), "executed more than once")
	require.Contains(t, strings.Join(gateCase.Reasons, "\n"), "no execution-count evidence")

	probeCounters["orientation_executed_incumbent_rows"] = 0
	probeCounters["orientation_root_probe_loops"] = 1
	probeCounters["orientation_suffix_probe_loops"] = 1
	probeCounters["orientation_incumbent_branch_loops"] = 1
	gateCase.Reasons = nil
	appendOrientationAttributionReasons(gateCase, diagnostic)
	require.Contains(t, gateCase.Reasons, "orientation incumbent arm performed work while the candidate was selected")
}

func TestResourceGateRequiresSingularInlineASPBranchAndInactiveArm(t *testing.T) {
	gateCase := &ResourceGateCase{}
	diagnostic := &TraversalExecutionDiagnostic{PlanReplay: &TraversalPlanReplayEvidence{Counters: map[string]int64{
		"asp_i1_candidate_marker_rows": 1,
		"asp_i1_fallback_marker_rows":  0,
		"asp_i1_candidate_branch_rows": 1,
		"asp_i1_fallback_branch_rows":  0,
	}}}
	appendInlineASPAttributionReasons(gateCase, diagnostic)
	require.Empty(t, gateCase.Reasons)

	diagnostic.PlanReplay.Counters["asp_i1_fallback_marker_rows"] = 1
	diagnostic.PlanReplay.Counters["asp_i1_fallback_branch_rows"] = 1
	appendInlineASPAttributionReasons(gateCase, diagnostic)
	require.Contains(t, gateCase.Reasons, "inline ASP execution must attribute exactly one candidate or fallback marker")
	require.Contains(t, gateCase.Reasons, "inline ASP fallback arm performed work while the candidate was selected")
}
