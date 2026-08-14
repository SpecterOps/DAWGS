// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

// TestResourceGateReportBindsExactInputArtifact verifies that schema v5 reports
// retain the SHA-256 digest of the exact JSONL bytes supplied to the gate.
func TestResourceGateReportBindsExactInputArtifact(t *testing.T) {
	tempDir := t.TempDir()
	artifact := filepath.Join(tempDir, "records.jsonl")
	record := CaseResult{
		Environment: &RunEnvironment{
			Round:    3,
			Block:    3,
			RunUUID:  "resource-run",
			Arm:      "candidate",
			ArmOrder: 2,
		},
		Dataset:       "fixture",
		Name:          "case",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
		Shape:         WorkloadShape{FixtureTier: "normal"},
		Optimization: &translate.OptimizationSummary{
			TargetOutcomes: []translate.TargetLoweringOutcome{{
				Family:  "SP",
				Applied: "SP-S4-C-D",
			}},
		},
		PostgresMetrics: &PostgresPlanMetrics{},
	}
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	artifactRaw, err := os.ReadFile(artifact)
	require.NoError(t, err)
	expectedDigest := sha256.Sum256(artifactRaw)

	output := filepath.Join(tempDir, "report.json")
	passed, err := createResourceGateReport(artifact, output)
	require.NoError(t, err)
	require.True(t, passed)

	var report ResourceGateReport
	reportRaw, err := os.ReadFile(output)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(reportRaw, &report))
	require.Equal(t, resourceGateVersion, report.Version)
	require.Equal(t, hex.EncodeToString(expectedDigest[:]), report.ArtifactSHA256)
	require.True(t, isLowerHexSHA256(report.ArtifactSHA256))
	require.Equal(t, 3, report.Cases[0].Round)
	require.Equal(t, 3, report.Cases[0].Block)
	require.Equal(t, "resource-run", report.Cases[0].RunUUID)
	require.Equal(t, "candidate", report.Cases[0].Arm)
	require.Equal(t, 2, report.Cases[0].ArmOrder)
}

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
			TargetOutcomes: []translate.TargetLoweringOutcome{{
				Family:  "SP",
				Applied: "SP-S4-C-D",
			}},
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
		Dataset:       "fixture",
		Name:          "candidate",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
		Shape: WorkloadShape{
			FixtureTier:         "envelope",
			FallbackExpectation: "forbidden",
		},
		Environment: &RunEnvironment{
			PoolSize:                  1,
			SessionMemoryCeilingBytes: 1 << 20,
			PoolMemoryCeilingBytes:    1 << 20,
		},
		TraversalTelemetry: &telemetry,
		PostgresMetrics:    &PostgresPlanMetrics{},
		Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
			Family:  "SP",
			Applied: "SP-B1-C-ALT-NODE-D",
		}},
		},
	}
	record.TraversalTelemetry.Diagnostic.Counters.Workspace = &TraversalWorkspaceCounters{
		SessionPeakBytes: telemetryInt64(4096),
		PoolPeakBytes:    telemetryInt64(4096),
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

	telemetry.Diagnostic.Counters.Ordinary.PeakState = telemetryInt64(-1)
	record.TraversalTelemetry = &telemetry
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	negativeReportPath := filepath.Join(t.TempDir(), "negative.json")
	passed, err = createResourceGateReport(artifact, negativeReportPath)
	require.NoError(t, err)
	require.False(t, passed)
	negativeReportRaw, err := os.ReadFile(negativeReportPath)
	require.NoError(t, err)
	var negativeReport ResourceGateReport
	require.NoError(t, json.Unmarshal(negativeReportRaw, &negativeReport))
	require.Contains(t, negativeReport.Cases[0].Reasons, "traversal counter state_rows=-1 is negative")

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
			Family:  "SP",
			Applied: "SP-B2-C-MIN-LEVEL-D",
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

// TestResourceGateRejectsDeclaredMemoryCeilingsWithoutMeasuredWorkspace verifies resource gate rejects declared memory ceilings without measured workspace behavior.
func TestResourceGateRejectsDeclaredMemoryCeilingsWithoutMeasuredWorkspace(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	record := CaseResult{
		Dataset:         "fixture",
		Name:            "declared-only",
		ExecutionMode:   ModePostgresSQL,
		Status:          StatusOK,
		Shape:           WorkloadShape{FixtureTier: "normal"},
		PostgresMetrics: &PostgresPlanMetrics{},
		Environment: &RunEnvironment{
			PoolSize:                  1,
			SessionMemoryCeilingBytes: 1024,
			PoolMemoryCeilingBytes:    4096,
		},
		Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
			Family:  "SP",
			Applied: "SP-S4-C-D",
		}}},
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

// TestResourceGateRequiresCompleteOrientationPolicyAndExactBranchAttribution verifies resource gate requires complete orientation policy and exact branch attribution behavior.
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
		Source:     "test",
		Counters:   map[string]int64{"orientation_executed_candidate_rows": 1},
		Flags:      map[string]bool{},
		Provenance: map[string]string{"counters.orientation_executed_candidate_rows": "test.marker"},
	}
	record := CaseResult{
		Dataset:            "fixture",
		Name:               "orientation",
		ExecutionMode:      ModePostgresSQL,
		Status:             StatusOK,
		Shape:              WorkloadShape{FixtureTier: "normal"},
		PostgresMetrics:    &PostgresPlanMetrics{},
		TraversalTelemetry: &telemetry,
		Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
			Family:        "fixed_suffix_expansion",
			Applied:       "EXPANSION-SUFFIX-SEEDED-REVERSE",
			EmittedPolicy: "orientation-probe-v1",
		}}},
	}
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	passed, err := createResourceGateReport(artifact, filepath.Join(t.TempDir(), "report.json"))
	require.NoError(t, err)
	require.False(t, passed)
}

// TestResourceGateScopesStressFallbackToDeclaredExpectation verifies resource gate scopes stress fallback to declared expectation behavior.
func TestResourceGateScopesStressFallbackToDeclaredExpectation(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	withoutExpectation := CaseResult{
		Dataset:         "fixture",
		Name:            "stress-no-overflow",
		ExecutionMode:   ModePostgresSQL,
		Status:          StatusOK,
		Shape:           WorkloadShape{FixtureTier: "stress"},
		PostgresMetrics: &PostgresPlanMetrics{},
		Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
			Family:  "SP",
			Applied: "SP-S0",
		}}},
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

// TestResourceGateValidatesExactOrientationMarkersAndProbeCounts verifies resource gate validates exact orientation markers and probe counts behavior.
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

// TestResourceGateValidatesSuffixGuardInactiveArmAndRejectsTopologyWork
// verifies the reverse-first guard's resource contract is independent from
// orientation-v2 and proves the unselected executor stayed inactive.
func TestResourceGateValidatesSuffixGuardInactiveArmAndRejectsTopologyWork(t *testing.T) {
	counters := map[string]int64{
		"suffix_guard_candidate_marker_rows": 1, "suffix_guard_fallback_marker_rows": 0,
		"suffix_guard_candidate_branch_rows": 1, "suffix_guard_fallback_branch_rows": 0,
		"suffix_guard_output_rows":              1,
		"suffix_guard_candidate_executor_loops": 1, "suffix_guard_fallback_executor_loops": 0,
	}
	diagnostic := &TraversalExecutionDiagnostic{PlanReplay: &TraversalPlanReplayEvidence{Counters: counters}}
	gateCase := &ResourceGateCase{}
	appendSuffixGuardAttributionReasons(gateCase, diagnostic)
	require.Empty(t, gateCase.Reasons)

	counters["suffix_guard_fallback_executor_loops"] = 1
	appendSuffixGuardAttributionReasons(gateCase, diagnostic)
	require.Contains(t, strings.Join(gateCase.Reasons, "\n"), "did not suppress the fallback executor")

	counters["suffix_guard_fallback_executor_loops"] = 0
	counters["orientation_forward_degree_rows"] = 10
	gateCase.Reasons = nil
	appendSuffixGuardAttributionReasons(gateCase, diagnostic)
	require.Contains(t, strings.Join(gateCase.Reasons, "\n"), "unexpectedly contains orientation topology work")
}

// TestResourceGateRequiresSingularInlineASPBranchAndInactiveArm verifies resource gate requires singular inline asp branch and inactive arm behavior.
func TestResourceGateRequiresSingularInlineASPBranchAndInactiveArm(t *testing.T) {
	gateCase := &ResourceGateCase{}
	diagnostic := &TraversalExecutionDiagnostic{PlanReplay: &TraversalPlanReplayEvidence{Counters: map[string]int64{
		"asp_i1_candidate_marker_rows":    1,
		"asp_i1_fallback_marker_rows":     0,
		"asp_i1_candidate_branch_rows":    1,
		"asp_i1_fallback_branch_rows":     0,
		"asp_i1_candidate_executor_loops": 1,
		"asp_i1_fallback_executor_loops":  0,
	}}}
	appendInlineASPAttributionReasons(gateCase, diagnostic)
	require.Empty(t, gateCase.Reasons)

	for _, missing := range []string{"asp_i1_candidate_branch_rows", "asp_i1_fallback_branch_rows"} {
		value := diagnostic.PlanReplay.Counters[missing]
		delete(diagnostic.PlanReplay.Counters, missing)
		missingCase := &ResourceGateCase{}
		appendInlineASPAttributionReasons(missingCase, diagnostic)
		require.Contains(t, missingCase.Reasons, "inline ASP execution is missing exact candidate or fallback output-branch row evidence")
		diagnostic.PlanReplay.Counters[missing] = value
	}
	for _, missing := range []string{"asp_i1_candidate_executor_loops", "asp_i1_fallback_executor_loops"} {
		value := diagnostic.PlanReplay.Counters[missing]
		delete(diagnostic.PlanReplay.Counters, missing)
		missingCase := &ResourceGateCase{}
		appendInlineASPAttributionReasons(missingCase, diagnostic)
		require.Contains(t, missingCase.Reasons, "inline ASP execution is missing exact candidate or fallback executor-loop evidence")
		diagnostic.PlanReplay.Counters[missing] = value
	}

	diagnostic.PlanReplay.Counters["asp_i1_fallback_executor_loops"] = 1
	executedInactiveCase := &ResourceGateCase{}
	appendInlineASPAttributionReasons(executedInactiveCase, diagnostic)
	require.Contains(t, executedInactiveCase.Reasons, "inline ASP fallback executor ran while the candidate was selected")
	diagnostic.PlanReplay.Counters["asp_i1_fallback_executor_loops"] = 0

	diagnostic.PlanReplay.Counters["asp_i1_fallback_marker_rows"] = 1
	diagnostic.PlanReplay.Counters["asp_i1_fallback_branch_rows"] = 1
	appendInlineASPAttributionReasons(gateCase, diagnostic)
	require.Contains(t, gateCase.Reasons, "inline ASP execution must attribute exactly one candidate or fallback marker")
	require.Contains(t, gateCase.Reasons, "inline ASP fallback output arm emitted rows while the candidate was selected")
}

// TestResourceGateScopesGuardedI1TelemetryAndInactiveArm verifies resource gate scopes guarded i1 telemetry and inactive arm behavior.
func TestResourceGateScopesGuardedI1TelemetryAndInactiveArm(t *testing.T) {
	require.False(t, telemetryRequiredForArchitecture(string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness)))
	require.False(t, telemetryRequiredForArchitecture(string(optimize.ShortestPathExecutorASPI1DAG)))
	require.True(t, telemetryRequiredForRecord(
		guardedI1ResourceRecord(string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness)),
		string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
	))

	gateCase := &ResourceGateCase{}
	diagnostic := &TraversalExecutionDiagnostic{PlanReplay: &TraversalPlanReplayEvidence{Counters: map[string]int64{
		"asp_i1_candidate_marker_rows":    1,
		"asp_i1_fallback_marker_rows":     0,
		"asp_i1_candidate_branch_rows":    1,
		"asp_i1_fallback_branch_rows":     0,
		"asp_i1_candidate_executor_loops": 1,
		"asp_i1_fallback_executor_loops":  0,
	}}}
	appendInlinePredecessorAttributionReasons(gateCase, diagnostic, "inline canonical SP")
	require.Empty(t, gateCase.Reasons)

	diagnostic.PlanReplay.Counters["asp_i1_fallback_marker_rows"] = 1
	diagnostic.PlanReplay.Counters["asp_i1_fallback_branch_rows"] = 1
	appendInlinePredecessorAttributionReasons(gateCase, diagnostic, "inline canonical SP")
	require.Contains(t, gateCase.Reasons, "inline canonical SP execution must attribute exactly one candidate or fallback marker")
	require.Contains(t, gateCase.Reasons, "inline canonical SP fallback output arm emitted rows while the candidate was selected")
}

// TestResourceGateDoesNotRequireGuardedTelemetryForExplicitI1References verifies resource gate does not require guarded telemetry for explicit i1 references behavior.
func TestResourceGateDoesNotRequireGuardedTelemetryForExplicitI1References(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "records.jsonl")
	record := CaseResult{
		Dataset:         "fixture",
		Name:            "explicit-references",
		ExecutionMode:   ModePostgresSQL,
		Status:          StatusOK,
		Shape:           WorkloadShape{FixtureTier: "normal"},
		PostgresMetrics: &PostgresPlanMetrics{},
		PostgresReferences: []PostgresReferenceResult{
			{
				Name:            "sp-i1-reference",
				Architecture:    string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
				FullComparator:  true,
				PostgresMetrics: &PostgresPlanMetrics{},
			},
			{
				Name:            "asp-i1-reference",
				Architecture:    string(optimize.ShortestPathExecutorASPI1DAG),
				FullComparator:  true,
				PostgresMetrics: &PostgresPlanMetrics{},
			},
		},
	}
	require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
	output := filepath.Join(t.TempDir(), "report.json")
	passed, err := createResourceGateReport(artifact, output)
	require.NoError(t, err)
	require.True(t, passed)

	var report ResourceGateReport
	raw, err := os.ReadFile(output)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &report))
	require.Len(t, report.Cases, 3)
	for _, gateCase := range report.Cases {
		require.True(t, gateCase.Passed, "%+v", gateCase)
		require.NotContains(t, gateCase.Reasons, "required traversal execution telemetry is missing")
	}
}

// TestResourceGateBindsGuardedI1PolicyAndCounterNamespace verifies resource gate binds guarded i1 policy and counter namespace behavior.
func TestResourceGateBindsGuardedI1PolicyAndCounterNamespace(t *testing.T) {
	tests := []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// architecture retains the architecture while anonymous record is assembled or evaluated.
		architecture string
		// mutate retains the mutate while anonymous record is assembled or evaluated.
		mutate func(*CaseResult)
		// passed indicates whether passed applies.
		passed bool
		// reason retains the reason while anonymous record is assembled or evaluated.
		reason string
	}{
		{
			name:         "canonical SP valid",
			architecture: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
			passed:       true,
		},
		{
			name:         "ASP valid",
			architecture: string(optimize.ShortestPathExecutorASPI1DAG),
			passed:       true,
		},
		{
			name:         "canonical SP missing outcome policy",
			architecture: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
			mutate: func(record *CaseResult) {
				record.Optimization.TargetOutcomes[0].EmittedPolicy = ""
			},
			reason: "inline canonical SP production architecture requires emitted policy",
		},
		{
			name:         "canonical SP wrong telemetry policy",
			architecture: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
			mutate: func(record *CaseResult) {
				record.TraversalTelemetry.Summary.EmittedIdentity = optimize.ShortestPathPolicyASPI1GuardedV1
			},
			reason: "inline canonical SP production telemetry requires emitted identity",
		},
		{
			name:         "canonical SP wrong counter namespace",
			architecture: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
			mutate: func(record *CaseResult) {
				diagnostic := record.TraversalTelemetry.Diagnostic
				diagnostic.Counters.InlineASP = diagnostic.Counters.InlineShortestPath
				diagnostic.Counters.InlineShortestPath = nil
				diagnostic.RequiredFamilies = []TraversalTelemetryFamily{TraversalTelemetryFamilyASP, TraversalTelemetryFamilyHydration}
				diagnostic.Provenance = guardedI1CounterProvenance("inline_asp")
			},
			reason: "inline canonical SP production telemetry requires inline_shortest_path counters",
		},
		{
			name:         "canonical SP missing contract counter family",
			architecture: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
			mutate: func(record *CaseResult) {
				record.TraversalTelemetry.Diagnostic.RequiredFamilies = []TraversalTelemetryFamily{TraversalTelemetryFamilyHydration}
			},
			reason: `inline canonical SP production telemetry requires declared counter family "shortest_path"`,
		},
		{
			name:         "ASP missing hydration family",
			architecture: string(optimize.ShortestPathExecutorASPI1DAG),
			mutate: func(record *CaseResult) {
				diagnostic := record.TraversalTelemetry.Diagnostic
				diagnostic.RequiredFamilies = []TraversalTelemetryFamily{TraversalTelemetryFamilyASP}
				diagnostic.Counters.Hydration = nil
			},
			reason: "inline ASP production telemetry requires declared hydration counters for its observation mode",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := guardedI1ResourceRecord(test.architecture)
			if test.mutate != nil {
				test.mutate(&record)
			}
			artifact := filepath.Join(t.TempDir(), "records.jsonl")
			require.NoError(t, writeJSONLFile(artifact, []CaseResult{record}))
			output := filepath.Join(t.TempDir(), "report.json")
			passed, err := createResourceGateReport(artifact, output)
			require.NoError(t, err)
			require.Equal(t, test.passed, passed)

			var report ResourceGateReport
			raw, err := os.ReadFile(output)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(raw, &report))
			require.Len(t, report.Cases, 1)
			if test.reason != "" {
				require.Contains(t, strings.Join(report.Cases[0].Reasons, "\n"), test.reason)
			}
		})
	}
}

// guardedI1ResourceRecord prepares or inspects test evidence for guarded i1 resource record.
func guardedI1ResourceRecord(architecture string) CaseResult {
	contract, _ := guardedInlineResourceContractForArchitecture(architecture)
	fallback := string(optimize.ShortestPathExecutorS4CanonicalWitness)
	requiredFamily := TraversalTelemetryFamilySP
	observationMode := "one_path"
	if architecture == string(optimize.ShortestPathExecutorASPI1DAG) {
		fallback = string(optimize.ShortestPathExecutorASPA1DAG)
		requiredFamily = TraversalTelemetryFamilyASP
		observationMode = "all_paths"
	}

	inlineCounters := &InlinePredecessorTraversalCounters{
		DistanceRows:           telemetryInt64(3),
		PredecessorRows:        telemetryInt64(2),
		EnumerationRows:        telemetryInt64(1),
		OutputPaths:            telemetryInt64(1),
		OutputBytes:            telemetryInt64(64),
		CandidateMarkerRows:    telemetryInt64(1),
		FallbackMarkerRows:     telemetryInt64(0),
		CandidateBranchRows:    telemetryInt64(1),
		FallbackBranchRows:     telemetryInt64(0),
		CandidateExecutorLoops: telemetryInt64(1),
		FallbackExecutorLoops:  telemetryInt64(0),
	}
	diagnosticCounters := TraversalDiagnosticCounters{}
	if requiredFamily == TraversalTelemetryFamilySP {
		diagnosticCounters.InlineShortestPath = inlineCounters
	} else {
		diagnosticCounters.InlineASP = inlineCounters
	}
	diagnosticCounters.Hydration = &TraversalHydrationCounters{
		PathCount:   telemetryInt64(1),
		NodeLookups: telemetryInt64(2),
		EdgeLookups: telemetryInt64(1),
		Loops:       telemetryInt64(1),
		Rows:        telemetryInt64(1),
		TimeNS:      telemetryInt64(100),
		Bytes:       telemetryInt64(64),
	}
	planCounters := map[string]int64{
		"asp_i1_distance_rows":            3,
		"asp_i1_predecessor_rows":         2,
		"asp_i1_enumeration_rows":         1,
		"asp_i1_output_rows":              1,
		"asp_i1_candidate_marker_rows":    1,
		"asp_i1_fallback_marker_rows":     0,
		"asp_i1_candidate_branch_rows":    1,
		"asp_i1_fallback_branch_rows":     0,
		"asp_i1_candidate_executor_loops": 1,
		"asp_i1_fallback_executor_loops":  0,
	}
	planProvenance := map[string]string{}
	for name := range planCounters {
		planProvenance["counters."+name] = "test.plan." + name
	}

	telemetry := validTraversalTelemetry()
	telemetry.Level = TraversalTelemetryLevelDiagnostic
	telemetry.Summary.RequestedIdentity = architecture
	telemetry.Summary.PlannedIdentities = []string{architecture, fallback}
	telemetry.Summary.EmittedIdentity = contract.policy
	telemetry.Summary.RuntimeIdentity = architecture
	telemetry.Summary.AppliedIdentity = architecture
	telemetry.Summary.ObservationMode = observationMode
	telemetry.Summary.RuntimeOutcomeAvailable = telemetryBool(true)
	telemetry.Summary.Caps = map[string]int64{
		"state_rows": 100, "predecessor_rows": 100, "output_rows": 100, "output_bytes": 1024,
	}
	telemetry.Summary.Provenance["observation_mode"] = "test.observation"
	telemetry.Summary.Provenance["runtime_outcome_available"] = "test.receipt"
	for capName := range telemetry.Summary.Caps {
		telemetry.Summary.Provenance["caps."+capName] = "test.cap." + capName
	}
	telemetry.Diagnostic = &TraversalExecutionDiagnostic{
		InvocationID:     "guarded-i1-resource",
		ConnectionID:     "backend-1",
		TimedSample:      telemetryBool(false),
		RequiredFamilies: []TraversalTelemetryFamily{requiredFamily, TraversalTelemetryFamilyHydration},
		Counters:         diagnosticCounters,
		CounterStatus:    TraversalTelemetryCounterStatusComplete,
		PlanReplay: &TraversalPlanReplayEvidence{
			Source:     "test-plan",
			Counters:   planCounters,
			Provenance: planProvenance,
		},
		Provenance: guardedI1CounterProvenance(contract.namespace),
	}

	return CaseResult{
		Dataset:       "fixture",
		Name:          "guarded-i1",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
		Shape: WorkloadShape{
			FixtureTier:         "normal",
			FallbackExpectation: "forbidden",
		},
		PostgresMetrics:    &PostgresPlanMetrics{},
		TraversalTelemetry: &telemetry,
		Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
			Family:        contract.family,
			Candidate:     architecture,
			Selected:      architecture,
			Applied:       architecture,
			EmittedPolicy: contract.policy,
		}}},
	}
}

// inlineI1CounterProvenance prepares or inspects test evidence for inline i1 counter provenance.
func inlineI1CounterProvenance(namespace string) map[string]string {
	provenance := map[string]string{}
	for _, name := range []string{
		"distance_rows", "predecessor_rows", "enumeration_rows", "output_paths", "output_bytes",
		"candidate_marker_rows", "fallback_marker_rows", "candidate_branch_rows", "fallback_branch_rows",
		"candidate_executor_loops", "fallback_executor_loops",
	} {
		provenance[namespace+"."+name] = "test." + namespace + "." + name
	}
	return provenance
}

// guardedI1CounterProvenance prepares or inspects test evidence for guarded i1 counter provenance.
func guardedI1CounterProvenance(namespace string) map[string]string {
	provenance := inlineI1CounterProvenance(namespace)
	for _, name := range []string{"path_count", "node_lookups", "edge_lookups", "loops", "rows", "time_ns", "bytes"} {
		provenance["hydration."+name] = "test.hydration." + name
	}
	return provenance
}
