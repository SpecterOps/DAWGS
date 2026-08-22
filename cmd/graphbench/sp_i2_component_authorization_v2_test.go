// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/stretchr/testify/require"
)

func TestValidateSPI2V2ComponentAuthorization(t *testing.T) {
	protocolSHA256 := strings.Repeat("a", 64)
	authorization := validSPI2V2ComponentAuthorization(protocolSHA256)
	require.NoError(t, validateSPI2V2ComponentAuthorization(authorization, protocolSHA256))

	mutations := map[string]func(*SPI2V2ComponentAuthorization){
		"wrong generation": func(value *SPI2V2ComponentAuthorization) { value.Generation = spI2GenerationV1 },
		"wrong protocol":   func(value *SPI2V2ComponentAuthorization) { value.ProtocolDeclarationSHA256 = strings.Repeat("b", 64) },
		"wrong combined arm": func(value *SPI2V2ComponentAuthorization) {
			value.AuthorizedExecutor = string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1)
		},
		"failed authorization": func(value *SPI2V2ComponentAuthorization) { value.Passed = false },
		"missing component":    func(value *SPI2V2ComponentAuthorization) { value.Components = value.Components[:1] },
		"reordered component": func(value *SPI2V2ComponentAuthorization) {
			value.Components[0], value.Components[1] = value.Components[1], value.Components[0]
		},
		"failed semantic check": func(value *SPI2V2ComponentAuthorization) { value.Components[0].SemanticPassed = false },
		"failed plan check":     func(value *SPI2V2ComponentAuthorization) { value.Components[1].PlanPassed = false },
		"wrong case count":      func(value *SPI2V2ComponentAuthorization) { value.Components[0].Cases = 5 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			copy := authorization
			copy.Components = append([]SPI2V2ComponentAuthorizationCase(nil), authorization.Components...)
			mutate(&copy)
			require.Error(t, validateSPI2V2ComponentAuthorization(copy, protocolSHA256))
		})
	}
}

func TestLoadSPI2V2ComponentAuthorizationStrictlyRejectsTampering(t *testing.T) {
	protocolSHA256 := strings.Repeat("a", 64)
	authorization := validSPI2V2ComponentAuthorization(protocolSHA256)
	path := filepath.Join(t.TempDir(), "authorization.json")
	require.NoError(t, writeIndentedJSON(path, authorization))
	_, err := loadSPI2V2ComponentAuthorization(path, protocolSHA256)
	require.NoError(t, err)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	for name, mutated := range map[string]string{
		"duplicate": strings.Replace(string(raw), `"schema":`, `"schema": "duplicate", "schema":`, 1),
		"unknown":   strings.Replace(string(raw), "{", `{"unknown":true,`, 1),
		"trailing":  string(raw) + `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			mutatedPath := filepath.Join(t.TempDir(), name+".json")
			require.NoError(t, os.WriteFile(mutatedPath, []byte(mutated), 0o600))
			_, err := loadSPI2V2ComponentAuthorization(mutatedPath, protocolSHA256)
			require.Error(t, err)
		})
	}
}

func TestCreateSPI2V2ComponentAuthorizationFromExactEvidence(t *testing.T) {
	e1d := spI2V2ComponentTestRecords(t, optimize.ShortestPathExecutorI2GuardedDistanceV2E1D)
	e1p := spI2V2ComponentTestRecords(t, optimize.ShortestPathExecutorI2GuardedDistanceV2E1P)
	for executor, records := range map[optimize.ShortestPathExecutor][]CaseResult{
		optimize.ShortestPathExecutorI2GuardedDistanceV2E1D: e1d,
		optimize.ShortestPathExecutorI2GuardedDistanceV2E1P: e1p,
	} {
		component, _, err := validateSPI2V2ComponentEvidence(records, executor)
		require.NoError(t, err)
		require.True(t, component.SemanticPassed)
		require.True(t, component.PlanPassed)
	}
	directory := t.TempDir()
	e1dPath := filepath.Join(directory, "e1d.jsonl")
	e1pPath := filepath.Join(directory, "e1p.jsonl")
	output := filepath.Join(directory, "authorization.json")
	require.NoError(t, writeJSONLFile(e1dPath, e1d))
	require.NoError(t, writeJSONLFile(e1pPath, e1p))
	passed, err := createSPI2V2ComponentAuthorization("../../benchmark/testdata/scale", e1dPath, e1pPath, output)
	require.NoError(t, err)
	require.True(t, passed)

	_, protocolSHA256, err := loadSPI2ProtocolV2("../../benchmark/testdata/scale/protocols/sp_i2_distance_v2.json")
	require.NoError(t, err)
	authorization, err := loadSPI2V2ComponentAuthorization(output, protocolSHA256)
	require.NoError(t, err)
	require.Equal(t, string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1DP), authorization.AuthorizedExecutor)
}

func TestValidateSPI2V2ComponentEvidenceRejectsSemanticPlanAndReceiptTampering(t *testing.T) {
	executor := optimize.ShortestPathExecutorI2GuardedDistanceV2E1D
	tests := map[string]func([]CaseResult) []CaseResult{
		"missing case": func(records []CaseResult) []CaseResult { return records[:len(records)-1] },
		"changed observation": func(records []CaseResult) []CaseResult {
			records[0].ObservedRows = []string{"[999]"}
			return records
		},
		"missing canonical plan": func(records []CaseResult) []CaseResult {
			records[0].PostgresPlanJSON = nil
			return records
		},
		"fallback receipt": func(records []CaseResult) []CaseResult {
			fallback := true
			records[0].TraversalTelemetry.Summary.FallbackExecuted = &fallback
			return records
		},
		"replayed invocation": func(records []CaseResult) []CaseResult {
			first := records[0].Stats.Samples[0].RuntimeInvocationID
			records[1].Stats.Samples[0].RuntimeInvocationID = first
			records[1].Stats.Samples[0].RuntimeReceiptEvents[0].InvocationID = first
			return records
		},
		"direct recursion": func(records []CaseResult) []CaseResult {
			for index := range records {
				if strings.Contains(records[index].Name, "cycle-control") {
					records[index].TraversalTelemetry.Diagnostic.PlanReplay.Counters["recursive_rows"] = 1
					break
				}
			}
			return records
		},
		"missing direct zero counter": func(records []CaseResult) []CaseResult {
			for index := range records {
				if strings.Contains(records[index].Name, "cycle-control") {
					delete(records[index].TraversalTelemetry.Diagnostic.PlanReplay.Counters, "recursive_loops")
					delete(records[index].TraversalTelemetry.Diagnostic.PlanReplay.Provenance, "counters.recursive_loops")
					break
				}
			}
			return records
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			records := mutate(spI2V2ComponentTestRecords(t, executor))
			_, _, err := validateSPI2V2ComponentEvidence(records, executor)
			require.Error(t, err)
		})
	}
}

func validSPI2V2ComponentAuthorization(protocolSHA256 string) SPI2V2ComponentAuthorization {
	component := func(executor, digest string) SPI2V2ComponentAuthorizationCase {
		return SPI2V2ComponentAuthorizationCase{
			Executor: executor, ArtifactSHA256: digest, Cases: 6,
			SemanticPassed: true, PlanPassed: true, ResourcePassed: true, ReceiptPassed: true,
			FallbackFree: true, CanonicalPlanSeen: true,
		}
	}
	return SPI2V2ComponentAuthorization{
		Schema:                    spI2V2ComponentAuthorizationSchema,
		Generation:                spI2GenerationV2,
		ProtocolDeclarationSHA256: protocolSHA256,
		SourceCommit:              "deadbeef",
		DirtyDiffSHA256:           strings.Repeat("d", 64),
		BinarySHA256:              strings.Repeat("b", 64),
		AuthorizedExecutor:        string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1DP),
		Components: []SPI2V2ComponentAuthorizationCase{
			component(string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1D), strings.Repeat("1", 64)),
			component(string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1P), strings.Repeat("2", 64)),
		},
		Passed: true,
	}
}

func spI2V2ComponentTestRecords(t *testing.T, executor optimize.ShortestPathExecutor) []CaseResult {
	t.Helper()
	full, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	selected, selection, err := selectRunnableScaleCorpusWithSPI2Protection(full, CorpusSelectors{Tags: []string{spI2TrainingTag}})
	require.NoError(t, err)
	require.Len(t, selected.Cases, 6)
	records := make([]CaseResult, 0, len(selected.Cases))
	for _, testCase := range selected.Cases {
		fixture, err := fixtureMetadata("unused", testCase.Dataset)
		require.NoError(t, err)
		fixture.PhysicalValidated = true
		fixture.PhysicalNodeCount = int64(fixture.NodeCount)
		fixture.PhysicalEdgeCount = int64(fixture.EdgeCount)
		fixture.NodeRelationBytes = int64(fixture.NodeCount) * 1024
		fixture.EdgeRelationBytes = int64(fixture.EdgeCount) * 1024
		_, record := spI2QualificationTestRecords(t, testCase, fixture, selection, spI2TrainingCorpusSHA256, 1, 1, 1)
		record.Environment.Arm = string(executor)
		record.Environment.ArmOrder = 1
		record.Environment.RunUUID = "component-check"
		record.StableObservation = true
		record.PostgresPlanJSON = []byte(`[ {"Plan":{"Node Type":"Result"},"Planning Time":1.0} ]`)
		planningMS := 1.0
		record.PostgresMetrics.PlanningMS = &planningMS
		outcome := &record.Optimization.TargetOutcomes[0]
		outcome.Candidate = string(executor)
		outcome.Selected = string(executor)
		outcome.Applied = string(executor)
		outcome.EmittedPolicy = optimize.ShortestPathPolicyI2DistanceGuardedV2
		outcome.SelectorVersion = optimize.ShortestPathSelectorStaticV9HiddenFanInTail
		outcome.EmittedCandidates = []string{string(executor), string(optimize.ShortestPathExecutorS4CanonicalDistance)}

		direct := executor == optimize.ShortestPathExecutorI2GuardedDistanceV2E1D
		directHit := direct && strings.Contains(testCase.Name, "cycle-control")
		admissionRows := int64(1)
		if directHit {
			admissionRows = 0
			for index := range record.PostgresMetrics.PlanNodes {
				node := &record.PostgresMetrics.PlanNodes[index]
				switch node.SubplanName {
				case "CTE sp_i2_distance_bounded", "CTE sp_i2_target":
					node.ActualRows = 0
				}
			}
			record.PostgresMetrics.RecursiveRows = 0
			record.PostgresMetrics.RecursiveLoops = 0
		}
		record.PostgresMetrics.PlanNodes = append(record.PostgresMetrics.PlanNodes, PostgresPlanNodeMetric{
			PlanNodeID: int64(len(record.PostgresMetrics.PlanNodes) + 1), NodeType: "Result",
			SubplanName: "CTE sp_i2_admission", ActualRows: admissionRows, ActualLoops: admissionRows,
		})
		if direct {
			directRows := int64(0)
			if directHit {
				directRows = 1
			}
			record.PostgresMetrics.PlanNodes = append(record.PostgresMetrics.PlanNodes, PostgresPlanNodeMetric{
				PlanNodeID: int64(len(record.PostgresMetrics.PlanNodes) + 1), NodeType: "Result",
				SubplanName: "CTE sp_i2_v2_direct", ActualRows: directRows, ActualLoops: 1,
			})
		}
		telemetry, err := buildPostgresCaseTraversalTelemetry(*record.Optimization, *record.PostgresMetrics, "101", TraversalTelemetryLevelDiagnostic)
		require.NoError(t, err)
		enrichInlineDistanceTraversalTelemetry(telemetry, record.RowCount)
		if directHit {
			for _, counter := range []string{"recursive_rows", "recursive_loops", "reverse_edge_probe_loops"} {
				telemetry.Diagnostic.PlanReplay.Counters[counter] = 0
				telemetry.Diagnostic.PlanReplay.Provenance["counters."+counter] = "test.plan." + counter
			}
		}
		require.NoError(t, telemetry.Validate())
		record.TraversalTelemetry = telemetry

		branch := telemetry.Summary.RuntimeBranch
		invocationID := fmt.Sprintf("component-%s-%s-1", executor, testCase.Name)
		fallback := false
		record.Stats.Samples = []LatencySample{{
			Round: 1, Block: 1, Arm: string(executor), ArmOrder: 1, RunUUID: "component-check", Iteration: 1,
			Case: testCase.Name, Dataset: testCase.Dataset, Backend: ModePostgresSQL, ConnectionID: "101",
			Classification: "warm", Duration: time.Millisecond, RequestedIdentity: string(executor),
			RuntimeIdentity: string(executor), RuntimeBranch: branch, FallbackExecuted: &fallback,
			RuntimeAttestation: "timed_invocation", RuntimeInvocationID: invocationID,
			RuntimeReceiptEvents: []RuntimeReceiptEvent{{
				InvocationID: invocationID, Ordinal: 1, RuntimeIdentity: string(executor), RuntimeBranch: branch,
			}},
		}}
		receiptID := fmt.Sprintf("component-%s-%s-stabilization", executor, testCase.Name)
		record.Stats.ReceiptStabilization = &RuntimeStabilizationReceipt{
			InvocationID: receiptID, RequestedIdentity: string(executor), RuntimeIdentity: string(executor),
			RuntimeBranch: branch, FallbackExecuted: &fallback,
			Events: []RuntimeReceiptEvent{{InvocationID: receiptID, Ordinal: 1, RuntimeIdentity: string(executor), RuntimeBranch: branch}},
		}
		records = append(records, record)
	}
	return records
}
