// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	pgdriver "github.com/specterops/dawgs/drivers/pg"
	"github.com/stretchr/testify/require"
)

const (
	operationalTestCandidate         = "SP-I2-C-D"
	operationalTestFallback          = "SP-S4-C-D"
	operationalTestTerminal          = "SP-S3-U-E+MAT-M0"
	operationalTestCypher            = "MATCH p = shortestPath((r)<-[:Traverse*1..32]-(e)) WHERE id(r) = $root_id AND id(e) = $end_id RETURN length(p)"
	operationalTestSQL               = "select 1::int8 as distance"
	operationalTestOrientationCypher = "MATCH (r)-[:Expand*0..16]->()-[:EnterSuffix]->()-[:ContinueSuffix]->()-[:CompleteSuffix]->(e) WHERE id(r) = $root_id RETURN id(e)"

	operationalMainHelper       = "GRAPHBENCH_OPERATIONAL_GATE_MAIN_HELPER"
	operationalMainHelperInput  = "GRAPHBENCH_OPERATIONAL_GATE_MAIN_INPUT"
	operationalMainHelperOutput = "GRAPHBENCH_OPERATIONAL_GATE_MAIN_OUTPUT"
)

func passingPromotionOperationalReport(t *testing.T, identity PromotionEvidenceIdentity) OperationalGateReport {
	t.Helper()
	requirements := defaultOperationalGateRequirements(identity.Candidate, identity.FallbackExecutor)
	if runtimeIdentity, supported := operationalCandidateRuntimeIdentity(identity.Candidate); supported {
		requirements.CandidateRuntimeIdentity = runtimeIdentity
	}
	requirements.CandidateSQLFingerprint = identity.OperationalCandidateSQLSHA256
	records := operationalTestEvidence(identity)
	for index := range records {
		operationalTestBindRecordToIdentity(&records[index], identity)
	}
	return buildOperationalGateReport(identity, requirements, records)
}

func operationalTestRequirements() OperationalGateRequirements {
	requirements := defaultOperationalGateRequirements(operationalTestCandidate, operationalTestFallback)
	requirements.CandidateSQLFingerprint = operationalTestPromotionIdentity().OperationalCandidateSQLSHA256
	return requirements
}

// TestParseConfigAcceptsOperationalGateMode verifies the strict input/output
// pair selects a standalone operational-report mode.
func TestParseConfigAcceptsOperationalGateMode(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-operational-gate-input", "operational-input.json",
		"-operational-gate-output", "operational-report.json",
	}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, "operational-input.json", cfg.OperationalGateInput)
	require.Equal(t, "operational-report.json", cfg.OperationalGateOutput)
}

// TestParseConfigRejectsIncompleteOrMixedOperationalGate verifies neither
// half of the file contract nor another standalone report mode can be ignored.
func TestParseConfigRejectsIncompleteOrMixedOperationalGate(t *testing.T) {
	complete := []string{
		"-operational-gate-input", "operational-input.json",
		"-operational-gate-output", "operational-report.json",
	}
	for _, test := range []struct {
		args   []string
		reason string
	}{
		{args: []string{"-operational-gate-input", "operational-input.json"}, reason: "requires operational-gate-input and operational-gate-output"},
		{args: []string{"-operational-gate-output", "operational-report.json"}, reason: "requires operational-gate-input and operational-gate-output"},
		{args: append(append([]string(nil), complete...), "-resource-artifact", "resource.jsonl"), reason: "mutually exclusive"},
		{args: append(append([]string(nil), complete...), "-promotion-manifest", "promotion.json"), reason: "mutually exclusive"},
	} {
		_, err := parseConfig(test.args, func(string) string { return "" })
		require.ErrorContains(t, err, test.reason, test.args)
	}
}

// TestOperationalGateMainFailsClosed runs the real main dispatch in a child
// test process. The failing report must be persisted before main exits one.
func TestOperationalGateMainFailsClosed(t *testing.T) {
	if os.Getenv(operationalMainHelper) == "1" {
		os.Args = []string{
			"graphbench",
			"-operational-gate-input", os.Getenv(operationalMainHelperInput),
			"-operational-gate-output", os.Getenv(operationalMainHelperOutput),
		}
		main()
		return
	}

	identity := operationalTestPromotionIdentity()
	records := operationalTestEvidence(identity)
	records[0].Result.Environment.BinarySHA256 = strings.Repeat("f", 64)
	input := OperationalGateInput{
		Version:           operationalGateVersion,
		PromotionIdentity: identity,
		Requirements:      operationalTestRequirements(),
		Records:           records,
	}
	directory := t.TempDir()
	inputPath := filepath.Join(directory, "input.json")
	outputPath := filepath.Join(directory, "report.json")
	operationalTestWriteJSON(t, inputPath, input)

	command := exec.Command(os.Args[0], "-test.run=^TestOperationalGateMainFailsClosed$")
	command.Env = append(os.Environ(),
		operationalMainHelper+"=1",
		operationalMainHelperInput+"="+inputPath,
		operationalMainHelperOutput+"="+outputPath,
	)
	output, err := command.CombinedOutput()
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError, string(output))
	require.Equal(t, 1, exitError.ExitCode(), string(output))
	require.Contains(t, string(output), "operational gate failed")

	raw, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var report OperationalGateReport
	require.NoError(t, json.Unmarshal(raw, &report))
	require.False(t, report.Passed)
	require.True(t, operationalTestReportContains(report, "run binary does not match promotion identity"))
}

// TestOperationalGateAcceptsCompleteCandidateBoundEvidence verifies the full
// promotion matrix and every independent operational proof serialize as a
// manifest-consumable passing report.
func TestOperationalGateAcceptsCompleteCandidateBoundEvidence(t *testing.T) {
	identity := operationalTestPromotionIdentity()
	requirements := operationalTestRequirements()
	report := buildOperationalGateReport(identity, requirements, operationalTestEvidence(identity))

	require.True(t, report.Passed, "global=%v records=%v", report.Reasons, report.Records)
	require.Empty(t, report.Reasons)
	require.Equal(t, 27, report.Coverage.RequiredMatrixCells)
	require.Equal(t, 27, report.Coverage.ObservedMatrixCells)
	require.Empty(t, report.Coverage.MissingMatrixCells)
	require.True(t, report.Coverage.LowWorkMem)
	require.True(t, report.Coverage.CancellationReplay)
	require.True(t, report.Coverage.RepeatableReadWriter)
	require.True(t, report.Coverage.SessionIsolation)
	require.True(t, report.Coverage.ForcedOverflowFallback)
	require.Len(t, report.Records, 32)
	require.Equal(t, operationalGateVersion, report.Input.Version)
	require.Equal(t, identity, report.Input.PromotionIdentity)
	require.Equal(t, requirements, report.Input.Requirements)
	require.Len(t, report.Input.Records, 32)
	require.True(t, lowercaseSHA256(report.InputSHA256))
	require.NoError(t, validateRecomputedOperationalGateReport(report, identity))
	for _, record := range report.Records {
		require.True(t, record.Passed, "%s: %v", record.ID, record.Reasons)
	}

	path := filepath.Join(t.TempDir(), "operational.json")
	require.NoError(t, writeOperationalGateReport(path, report))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var document map[string]any
	require.NoError(t, json.Unmarshal(raw, &document))
	require.Equal(t, true, document["passed"])
	require.Contains(t, document, "promotion_identity")
	encodedIdentity, err := json.Marshal(document["promotion_identity"])
	require.NoError(t, err)
	var decoded PromotionEvidenceIdentity
	require.NoError(t, json.Unmarshal(encodedIdentity, &decoded))
	require.Equal(t, identity, decoded)
}

// TestOperationalGateRejectsSPI2PlanAttributionTampering proves every SP-I2
// operational record must independently bind its typed counters, exact named
// plan branches, inactive executor arm, and public output cardinality.
func TestOperationalGateRejectsSPI2PlanAttributionTampering(t *testing.T) {
	identity := operationalTestPromotionIdentity()
	tests := []struct {
		name   string
		mutate func([]OperationalEvidenceRecord)
		reason string
	}{
		{
			name: "summary-only pooled candidate",
			mutate: func(records []OperationalEvidenceRecord) {
				records[9].Result.TraversalTelemetry.Level = TraversalTelemetryLevelSummary
				records[9].Result.TraversalTelemetry.Diagnostic = nil
			},
			reason: "SP-I2 operational records require an untimed diagnostic replay",
		},
		{
			name: "missing candidate marker",
			mutate: func(records []OperationalEvidenceRecord) {
				delete(records[0].Result.TraversalTelemetry.Diagnostic.PlanReplay.Counters, "sp_i2_candidate_marker_rows")
			},
			reason: "missing exact plan counter sp_i2_candidate_marker_rows",
		},
		{
			name: "dual markers",
			mutate: func(records []OperationalEvidenceRecord) {
				telemetry := records[0].Result.TraversalTelemetry
				telemetry.Diagnostic.PlanReplay.Counters["sp_i2_fallback_marker_rows"] = 1
				value := int64(1)
				telemetry.Diagnostic.Counters.InlineShortestDistance.FallbackMarkerRows = &value
			},
			reason: "must attribute exactly one candidate or fallback marker",
		},
		{
			name: "candidate initializes fallback executor",
			mutate: func(records []OperationalEvidenceRecord) {
				telemetry := records[0].Result.TraversalTelemetry
				telemetry.Diagnostic.PlanReplay.Counters["sp_i2_fallback_executor_loops"] = 1
				value := int64(1)
				telemetry.Diagnostic.Counters.InlineShortestDistance.FallbackExecutorLoops = &value
			},
			reason: "candidate selection did not suppress the fallback executor and output arm",
		},
		{
			name: "candidate claims admission at cap plus one",
			mutate: func(records []OperationalEvidenceRecord) {
				telemetry := records[0].Result.TraversalTelemetry
				value := telemetry.Summary.Caps["state_rows"] + 1
				telemetry.Diagnostic.PlanReplay.Counters["sp_i2_distance_rows"] = value
				telemetry.Diagnostic.Counters.InlineShortestDistance.StateRows = &value
				telemetry.Diagnostic.Counters.InlineShortestDistance.FrontierRows = &value
			},
			reason: "candidate selection exceeds its state or conservative frontier cap",
		},
		{
			name: "typed plan drift",
			mutate: func(records []OperationalEvidenceRecord) {
				value := int64(3)
				records[0].Result.TraversalTelemetry.Diagnostic.Counters.InlineShortestDistance.StateRows = &value
			},
			reason: "typed counter does not match plan counter sp_i2_distance_rows",
		},
		{
			name: "candidate target drift",
			mutate: func(records []OperationalEvidenceRecord) {
				records[0].Result.TraversalTelemetry.Diagnostic.PlanReplay.Counters["sp_i2_target_rows"] = 0
			},
			reason: "candidate branch does not agree with its target receipt",
		},
		{
			name: "public output drift",
			mutate: func(records []OperationalEvidenceRecord) {
				records[0].Result.RowCount = 2
			},
			reason: "typed output does not match the exact public observation",
		},
		{
			name: "fallback initializes candidate executor",
			mutate: func(records []OperationalEvidenceRecord) {
				record := operationalTestScenario(records, OperationalScenarioForcedOverflow)
				telemetry := record.Result.TraversalTelemetry
				telemetry.Diagnostic.PlanReplay.Counters["sp_i2_candidate_executor_loops"] = 1
				value := int64(1)
				telemetry.Diagnostic.Counters.InlineShortestDistance.CandidateExecutorLoops = &value
			},
			reason: "fallback selection did not suppress the candidate executor and output arm",
		},
		{
			name: "fallback lacks cap plus one sentinel",
			mutate: func(records []OperationalEvidenceRecord) {
				record := operationalTestScenario(records, OperationalScenarioForcedOverflow)
				telemetry := record.Result.TraversalTelemetry
				value := telemetry.Summary.Caps["state_rows"]
				telemetry.Diagnostic.PlanReplay.Counters["sp_i2_distance_rows"] = value
				telemetry.Diagnostic.Counters.InlineShortestDistance.StateRows = &value
				telemetry.Diagnostic.Counters.InlineShortestDistance.FrontierRows = &value
			},
			reason: "fallback selection lacks an exact state or conservative frontier cap+1 sentinel",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			records := operationalTestEvidence(identity)
			test.mutate(records)
			report := buildOperationalGateReport(identity, operationalTestRequirements(), records)
			require.False(t, report.Passed)
			require.True(t, operationalTestReportContains(report, test.reason), "global=%v records=%v", report.Reasons, report.Records)
		})
	}
}

// TestValidateRecomputedOperationalGateReportRejectsTampering proves final
// promotion derives its decision from the embedded raw evidence. Each raw
// mutation refreshes the input digest to model an attacker who also rewrites
// that shallow checksum; the unchanged passing summary must still be rejected.
func TestValidateRecomputedOperationalGateReportRejectsTampering(t *testing.T) {
	identity := operationalTestPromotionIdentity()
	passing := buildOperationalGateReport(identity, operationalTestRequirements(), operationalTestEvidence(identity))
	require.True(t, passing.Passed, "global=%v records=%v", passing.Reasons, passing.Records)

	tests := []struct {
		name   string
		mutate func(*OperationalGateReport)
	}{
		{name: "sql", mutate: func(report *OperationalGateReport) {
			report.Input.Records[0].Result.SQL = "select 2::int8 as distance"
			report.Input.Records[0].Result.SQLFingerprint = sqlFingerprint(report.Input.Records[0].Result.SQL)
		}},
		{name: "optimization", mutate: func(report *OperationalGateReport) {
			report.Input.Records[0].Result.Optimization.TargetOutcomes[0].Applied = operationalTestFallback
		}},
		{name: "receipt", mutate: func(report *OperationalGateReport) {
			report.Input.Records[0].Result.Stats.Samples[0].RuntimeReceiptEvents[0].RuntimeIdentity = operationalTestFallback
		}},
		{name: "cancellation", mutate: func(report *OperationalGateReport) {
			operationalTestScenario(report.Input.Records, OperationalScenarioCancellation).Cancellation.ReplaySucceeded = false
		}},
		{name: "snapshot", mutate: func(report *OperationalGateReport) {
			operationalTestScenario(report.Input.Records, OperationalScenarioConcurrentWriter).Snapshot.ObservationAfterSHA256 = strings.Repeat("0", 64)
		}},
		{name: "session isolation", mutate: func(report *OperationalGateReport) {
			operationalTestScenario(report.Input.Records, OperationalScenarioSessionIsolation).SessionIsolation.SessionAObservedBRows = 1
		}},
		{name: "raw source binding", mutate: func(report *OperationalGateReport) {
			report.Input.Records[0].SourceSHA256 = strings.Repeat("0", 64)
		}},
		{name: "embedded identity", mutate: func(report *OperationalGateReport) {
			report.Input.PromotionIdentity.BinarySHA256 = strings.Repeat("0", 64)
		}},
		{name: "embedded requirements", mutate: func(report *OperationalGateReport) {
			report.Input.Requirements.CancellationMaximum = time.Second
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report := operationalTestCloneReport(t, passing)
			test.mutate(&report)
			var err error
			report.InputSHA256, err = operationalGateInputSHA256(report.Input)
			require.NoError(t, err)
			raw, err := json.Marshal(report)
			require.NoError(t, err)
			require.Error(t, validatePromotionOperationalReport(raw, identity))
		})
	}
}

// TestValidateRecomputedOperationalGateReportRejectsSummaryForgery verifies
// raw evidence cannot be paired with edited coverage, decisions, or a passing
// disposition, even when the embedded-input digest remains valid.
func TestValidateRecomputedOperationalGateReportRejectsSummaryForgery(t *testing.T) {
	identity := operationalTestPromotionIdentity()
	passing := buildOperationalGateReport(identity, operationalTestRequirements(), operationalTestEvidence(identity))
	tests := []struct {
		name   string
		mutate func(*OperationalGateReport)
		reason string
	}{
		{name: "coverage", mutate: func(report *OperationalGateReport) { report.Coverage.ObservedMatrixCells-- }, reason: "coverage differs"},
		{name: "record decision", mutate: func(report *OperationalGateReport) { report.Records[0].Passed = false }, reason: "record decisions differ"},
		{name: "passed", mutate: func(report *OperationalGateReport) { report.Passed = false }, reason: "passing disposition differs"},
		{name: "reasons", mutate: func(report *OperationalGateReport) { report.Reasons = []string{"forged"} }, reason: "passing disposition differs"},
		{name: "input digest", mutate: func(report *OperationalGateReport) { report.InputSHA256 = strings.Repeat("0", 64) }, reason: "SHA-256 does not match"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report := operationalTestCloneReport(t, passing)
			test.mutate(&report)
			raw, err := json.Marshal(report)
			require.NoError(t, err)
			require.ErrorContains(t, validatePromotionOperationalReport(raw, identity), test.reason)
		})
	}
}

// TestValidatePromotionOperationalReportRejectsAmbiguousEmbeddedEvidence
// keeps strict decoding at the final manifest boundary, including nested raw
// input fields and concatenated JSON documents.
func TestValidatePromotionOperationalReportRejectsAmbiguousEmbeddedEvidence(t *testing.T) {
	identity := operationalTestPromotionIdentity()
	report := buildOperationalGateReport(identity, operationalTestRequirements(), operationalTestEvidence(identity))
	raw, err := json.Marshal(report)
	require.NoError(t, err)

	var document map[string]any
	require.NoError(t, json.Unmarshal(raw, &document))
	document["input"].(map[string]any)["unexpected_raw_proof"] = true
	unknown, err := json.Marshal(document)
	require.NoError(t, err)
	require.ErrorContains(t, validatePromotionOperationalReport(unknown, identity), "unknown field")
	require.ErrorContains(t, validatePromotionOperationalReport(append(raw, []byte(`{}`)...), identity), "trailing JSON data")
}

// TestOperationalGateAcceptsProducerPhysicalSizeVariation verifies relation
// allocation diagnostics may vary between independent captures without
// changing the canonical logical fixture binding.
func TestOperationalGateAcceptsProducerPhysicalSizeVariation(t *testing.T) {
	identity := operationalTestPromotionIdentity()
	records := operationalTestEvidence(identity)
	for index := range records {
		records[index].Result.Fixture.NodeRelationBytes = int64(4096 + index*8192)
		records[index].Result.Fixture.EdgeRelationBytes = int64(8192 + index*16384)
	}
	report := buildOperationalGateReport(identity, operationalTestRequirements(), records)
	require.True(t, report.Passed, "global=%v records=%v", report.Reasons, report.Records)
}

// TestOperationalGateAuthorizesOrientationByTarget verifies a real
// fixed-suffix outer shape is not mistaken for the single variable-expansion
// target authorized by an orientation bucket.
func TestOperationalGateAuthorizesOrientationByTarget(t *testing.T) {
	query := "MATCH (r)-[:Expand*0..16]->()-[:EnterSuffix]->()-[:ContinueSuffix]->()-[:CompleteSuffix]->(e) WHERE id(r) = $root_id RETURN id(e)"
	digest := strings.Repeat("a", 64)
	policy := string(optimize.ExpansionSearchPolicyOrientationProbeV2)
	identity := PromotionEvidenceIdentity{
		Candidate: policy, SelectorVersion: policy, ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: string(optimize.ExpansionSearchStepwiseForward), SourceCommit: "deadbeef",
		SourceSHA256: digest, BinarySHA256: strings.Repeat("b", 64), CorpusSHA256: strings.Repeat("c", 64),
		OperationalCandidateSQLSHA256: sqlFingerprint(operationalTestSQL),
		Caps:                          orientationPromotionCaps(),
		Buckets: []PromotionBucket{{
			Name: "fixed-suffix", QuerySHA256: []string{pgdriver.TraversalPolicyQuerySHA256(query)},
			Direction: "outbound", ObservationMode: "endpoint_ids", MinimumDepth: 0, MaximumDepth: 16,
			RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"},
		}},
	}
	record := operationalTestRecord(operationalTestPromotionIdentity(), "orientation", OperationalScenarioCandidateMatrix, "auto", "4MB", 1)
	minimumDepth, maximumDepth := 0, 16
	record.Result.Cypher = query
	record.Result.Shape = WorkloadShape{
		QualificationSplit: "training", EdgeKinds: []string{"Expand", "EnterSuffix", "ContinueSuffix", "CompleteSuffix"},
		MinDepth: &minimumDepth, MaxDepth: &maximumDepth,
	}
	record.Result.TraversalTelemetry.Summary.ObservationMode = "endpoint_ids"
	eligible := true
	minimumTargetDepth, maximumTargetDepth := int64(0), int64(16)
	record.Result.Optimization = &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
		Lowering: optimize.LoweringExpansionSearchStrategy, TargetKind: "traversal", Family: "fixed_suffix_expansion",
		Candidate: string(optimize.ExpansionSearchSuffixSeededReverse), Selected: string(optimize.ExpansionSearchStepwiseForward),
		Applied: string(optimize.ExpansionSearchStepwiseForward), Fallback: string(optimize.ExpansionSearchStepwiseForward),
		PlannedCandidates: []string{string(optimize.ExpansionSearchStepwiseForward), string(optimize.ExpansionSearchSuffixSeededReverse)},
		EmittedCandidates: []string{string(optimize.ExpansionSearchStepwiseForward), string(optimize.ExpansionSearchSuffixSeededReverse)},
		EmittedPolicy:     policy, SelectorVersion: policy, ExecutionBoundary: "guarded_dual_arm", SelectionMode: "production_canary",
		ObservationMode: "endpoint_ids", MinimumDepth: &minimumTargetDepth, MaximumDepth: &maximumTargetDepth,
		Eligible: &eligible, StaticallyEligible: &eligible,
		EligibilityFacts: []translate.TargetEligibilityFact{{Name: "qualified_fixed_suffix_topology", Eligible: true}},
		ProbeCaps: &optimize.ExpansionSearchProbeCaps{
			RootRowLimit:              optimize.ExpansionSearchOrientationRootRowLimit,
			ReverseSeedRowLimit:       optimize.ExpansionSearchOrientationReverseSeedRowLimit,
			DirectionalDegreeRowLimit: optimize.ExpansionSearchOrientationDirectionalDegreeRowLimit,
		},
		Admission: &optimize.ExpansionSearchAdmission{
			StateLimit: optimize.ExpansionSearchOrientationStateLimit, RequiresCompleteProbes: true,
			FallbackStrategy: optimize.ExpansionSearchStepwiseForward,
		},
		StateLimit: optimize.ExpansionSearchOrientationStateLimit,
	}}}
	requirements := defaultOperationalGateRequirements(string(optimize.ExpansionSearchSuffixSeededReverse), string(optimize.ExpansionSearchStepwiseForward))
	requirements.CandidateSQLFingerprint = record.Result.SQLFingerprint
	identity.OperationalCandidateSQLSHA256 = record.Result.SQLFingerprint
	require.Empty(t, validateOperationalAuthorizedWorkload(identity, requirements, record))

	record.Result.Optimization.TargetOutcomes[0].EligibilityFacts[0].Eligible = false
	require.Contains(t, validateOperationalAuthorizedWorkload(identity, requirements, record), "operational orientation target differs from its authorized promotion bucket")
}

// TestOperationalGateRejectsUnregisteredGuardPolicy verifies a caller cannot
// invent the relationship between a policy identity and a runtime arm.
func TestOperationalGateRejectsUnregisteredGuardPolicy(t *testing.T) {
	identity := operationalTestPromotionIdentity()
	identity.Candidate = "suffix-reverse-guard-v1"
	records := operationalTestEvidence(identity)
	for index := range records {
		records[index].Result.TraversalTelemetry.Summary.EmittedIdentity = identity.Candidate
	}
	requirements := operationalTestRequirements()

	report := buildOperationalGateReport(identity, requirements, records)
	require.False(t, report.Passed)
	require.Contains(t, report.Reasons, "promotion candidate has no registered operational runtime mapping")
}

// TestOperationalCandidateRuntimeIdentityFreezesPolicyMapping verifies policy
// identities cannot choose an arbitrary exact traversal arm.
func TestOperationalCandidateRuntimeIdentityFreezesPolicyMapping(t *testing.T) {
	for _, policy := range []string{
		string(optimize.ExpansionSearchPolicyOrientationProbeV1),
		string(optimize.ExpansionSearchPolicyOrientationProbeV2),
	} {
		actual, supported := operationalCandidateRuntimeIdentity(policy)
		require.True(t, supported)
		require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), actual)
	}
	actual, supported := operationalCandidateRuntimeIdentity("suffix-reverse-guard-v1")
	require.False(t, supported)
	require.Empty(t, actual)
}

// TestCreateOperationalGateReport verifies the file boundary preserves the
// exact source document identity and writes failed gates as useful evidence.
func TestCreateOperationalGateReport(t *testing.T) {
	identity := operationalTestPromotionIdentity()
	input := OperationalGateInput{
		Version:           operationalGateVersion,
		PromotionIdentity: identity,
		Requirements:      operationalTestRequirements(),
		Records:           operationalTestEvidence(identity),
	}
	inputPath := filepath.Join(t.TempDir(), "operational-input.json")
	outputPath := filepath.Join(t.TempDir(), "operational-report.json")
	operationalTestWriteJSON(t, inputPath, input)

	passed, err := createOperationalGateReport(inputPath, outputPath)
	require.NoError(t, err)
	require.True(t, passed)
	output, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var report OperationalGateReport
	require.NoError(t, json.Unmarshal(output, &report))
	require.True(t, report.Passed, report.Reasons)
	require.Equal(t, identity, report.PromotionIdentity)

	input.Records[0].Result.Environment.BinarySHA256 = strings.Repeat("f", 64)
	operationalTestWriteJSON(t, inputPath, input)
	passed, err = createOperationalGateReport(inputPath, outputPath)
	require.NoError(t, err)
	require.False(t, passed)
	output, err = os.ReadFile(outputPath)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(output, &report))
	require.False(t, report.Passed)
	require.True(t, operationalTestReportContains(report, "run binary does not match promotion identity"))
}

// TestCreateOperationalGateReportPreservesInput rejects an output path that
// would replace the immutable evidence document.
func TestCreateOperationalGateReportPreservesInput(t *testing.T) {
	identity := operationalTestPromotionIdentity()
	input := OperationalGateInput{
		Version:           operationalGateVersion,
		PromotionIdentity: identity,
		Requirements:      operationalTestRequirements(),
		Records:           operationalTestEvidence(identity),
	}
	path := filepath.Join(t.TempDir(), "operational-input.json")
	operationalTestWriteJSON(t, path, input)

	_, err := createOperationalGateReport(path, path)
	require.ErrorContains(t, err, "distinct paths")
	loaded, err := loadOperationalGateInput(path)
	require.NoError(t, err)
	require.Equal(t, identity, loaded.PromotionIdentity)
}

// TestLoadOperationalGateInputRejectsAmbiguousJSON verifies the loader fails
// closed on schema drift and concatenated documents.
func TestLoadOperationalGateInputRejectsAmbiguousJSON(t *testing.T) {
	identity := operationalTestPromotionIdentity()
	valid := OperationalGateInput{
		Version:           operationalGateVersion,
		PromotionIdentity: identity,
		Requirements:      operationalTestRequirements(),
		Records:           operationalTestEvidence(identity),
	}
	validJSON, err := json.Marshal(valid)
	require.NoError(t, err)

	for _, test := range []struct {
		name   string
		input  string
		reason string
	}{
		{name: "unknown field", input: strings.TrimSuffix(string(validJSON), "}") + `,"unexpected":true}`, reason: "unknown field"},
		{name: "duplicate field", input: strings.Replace(string(validJSON), `"version":2`, `"version":2,"version":2`, 1), reason: "duplicate JSON object key"},
		{name: "nested duplicate field", input: strings.Replace(string(validJSON), `"candidate_runtime_identity":"`+operationalTestCandidate+`"`, `"candidate_runtime_identity":"`+operationalTestCandidate+`","candidate_runtime_identity":"`+operationalTestCandidate+`"`, 1), reason: "duplicate JSON object key"},
		{name: "trailing document", input: string(validJSON) + `{}`, reason: "trailing JSON data"},
		{name: "unsupported version", input: strings.Replace(string(validJSON), `"version":2`, `"version":1`, 1), reason: "input version must be 2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "input.json")
			require.NoError(t, os.WriteFile(path, []byte(test.input), 0o600))
			_, err := loadOperationalGateInput(path)
			require.ErrorContains(t, err, test.reason)
		})
	}
	_, err = loadOperationalGateInput("")
	require.ErrorContains(t, err, "explicit input path")
}

// TestOperationalGateFailsClosedOnMissingOrContradictoryEvidence verifies each
// class of operational proof is substantive rather than a presence-only flag.
func TestOperationalGateFailsClosedOnMissingOrContradictoryEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]OperationalEvidenceRecord) []OperationalEvidenceRecord
		reason string
	}{
		{
			name: "matrix cell",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				return records[1:]
			},
			reason: "candidate matrix is missing pool_size=1 concurrency=1 plan_cache_mode=auto",
		},
		{
			name: "duplicate matrix cell",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				duplicate := records[0]
				duplicate.ID = "duplicate-matrix"
				return append(records, duplicate)
			},
			reason: "candidate matrix cell is duplicated",
		},
		{
			name: "duplicate exceptional scenario",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := *operationalTestScenario(records, OperationalScenarioLowWorkMem)
				record.ID = "duplicate-low-memory"
				return append(records, record)
			},
			reason: "operational scenario is duplicated",
		},
		{
			name: "promotion identity",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].PromotionIdentity.BinarySHA256 = strings.Repeat("f", 64)
				return records
			},
			reason: "record promotion identity does not match report",
		},
		{
			name: "source archive",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].SourceSHA256 = strings.Repeat("f", 64)
				return records
			},
			reason: "record source archive does not match promotion identity",
		},
		{
			name: "binary",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].Result.Environment.BinarySHA256 = strings.Repeat("f", 64)
				return records
			},
			reason: "run binary does not match promotion identity",
		},
		{
			name: "dirty source",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].Result.Environment.DirtyDiffSHA256 = strings.Repeat("d", 64)
				return records
			},
			reason: "operational evidence was captured from a dirty source tree",
		},
		{
			name: "candidate receipt",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				sample := &records[0].Result.Stats.Samples[0]
				sample.RuntimeReceiptEvents = nil
				return records
			},
			reason: "event chain is missing",
		},
		{
			name: "fabricated pooled receipt",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := &records[9]
				record.Result.Stats.Samples = []LatencySample{operationalTestCandidateSample(record.Result, "forged-pooled", "101")}
				return records
			},
			reason: "pooled candidate sample must retain non-attested GraphBench replay metadata",
		},
		{
			name: "matrix declaration",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].Concurrency = 7
				records[0].Result.Concurrency[0].Concurrency = 7
				return records
			},
			reason: "candidate matrix record is outside the required matrix",
		},
		{
			name: "matrix workers",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := &records[3]
				record.Result.Concurrency[0].Samples[1].Worker = 1
				return records
			},
			reason: "concurrency block duplicates a worker iteration",
		},
		{
			name: "stable observation",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].Result.StableObservation = false
				return records
			},
			reason: "operational record lacks a stable observation",
		},
		{
			name: "resolved endpoint substitution",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioCancellation)
				record.Result.Params["end_id"] = int64(999)
				return records
			},
			reason: "operational scenarios do not use one exact authorized workload",
		},
		{
			name: "symbolic endpoint substitution",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioSessionIsolation)
				record.Result.NodeParams["end_id"] = "easy-end"
				return records
			},
			reason: "operational scenarios do not use one exact authorized workload",
		},
		{
			name: "symbolic endpoint list substitution",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioConcurrentWriter)
				record.Result.NodeListParams["targets"] = []string{"easy-end"}
				return records
			},
			reason: "operational scenarios do not use one exact authorized workload",
		},
		{
			name: "unauthorized query",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].Result.Cypher += " LIMIT 1"
				return records
			},
			reason: "operational query must match exactly one promotion bucket, matched 0",
		},
		{
			name: "sql fingerprint",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].Result.SQL += " where false"
				return records
			},
			reason: "operational SQL fingerprint is missing or does not bind the measured SQL",
		},
		{
			name: "self-consistent substituted SQL",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := &records[0]
				record.Result.SQL = "select 0::int8 as distance"
				record.Result.SQLFingerprint = sqlFingerprint(record.Result.SQL)
				return records
			},
			reason: "non-overflow operational scenarios do not use one exact candidate SQL",
		},
		{
			name: "global self-consistent substituted SQL",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				for index := range records {
					records[index].Result.SQL = "select 0::int8 as distance"
					records[index].Result.SQLFingerprint = sqlFingerprint(records[index].Result.SQL)
				}
				return records
			},
			reason: "operational SQL fingerprint differs from the independently frozen production candidate SQL",
		},
		{
			name: "missing optimization target",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].Result.Optimization = nil
				return records
			},
			reason: "operational record lacks optimization target evidence",
		},
		{
			name: "optimization selector substitution",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].Result.Optimization.TargetOutcomes[0].SelectorVersion = "unregistered-selector"
				return records
			},
			reason: "operational optimization target does not prove the exact production candidate policy",
		},
		{
			name: "optimization bucket substitution",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].Result.Optimization.TargetOutcomes[0].Direction = "outbound"
				return records
			},
			reason: "operational optimization target differs from its authorized promotion bucket",
		},
		{
			name: "optimization cap substitution",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].Result.Optimization.TargetOutcomes[0].StateLimit--
				return records
			},
			reason: "operational optimization target cap differs from promotion identity: state_limit",
		},
		{
			name: "authorized shape",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				*records[0].Result.Shape.MaxDepth = 31
				return records
			},
			reason: "operational workload shape differs from its authorized promotion bucket",
		},
		{
			name: "physical fixture",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].Result.Fixture.PhysicalValidated = false
				return records
			},
			reason: "operational record lacks one physically validated fixture identity",
		},
		{
			name: "forged physical cardinality",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[0].Result.Fixture.PhysicalEdgeCount++
				return records
			},
			reason: "operational record lacks one physically validated fixture identity",
		},
		{
			name: "fixture configuration substitution",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioLowWorkMem)
				record.Result.Fixture.Configuration = "easier-fixture"
				return records
			},
			reason: "operational scenarios do not use one exact authorized workload",
		},
		{
			name: "cross-scenario workload",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioCancellation)
				record.Result.WorkloadSHA256 = strings.Repeat("5", 64)
				return records
			},
			reason: "operational scenarios do not use one exact authorized workload",
		},
		{
			name: "low work mem",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioLowWorkMem)
				record.Result.PostgresEnvironment.WorkMem = "65kB"
				return records
			},
			reason: "work_mem exceeds constrained ceiling 65536 bytes",
		},
		{
			name: "cancellation latency",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioCancellation)
				record.Cancellation.Latency = 250 * time.Millisecond
				return records
			},
			reason: "cancellation latency must be positive and below 250ms",
		},
		{
			name: "rollback reuse",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioCancellation)
				record.Cancellation.ReplayBackendPID++
				return records
			},
			reason: "post-rollback replay did not reuse the cancelled backend PID",
		},
		{
			name: "repeatable read writer",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioConcurrentWriter)
				record.Snapshot.ObservationAfterSHA256 = strings.Repeat("f", 64)
				return records
			},
			reason: "reader observation changed across the concurrent commit",
		},
		{
			name: "concurrent writer effect",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioConcurrentWriter)
				record.Snapshot.WriterAffectedRows = 0
				return records
			},
			reason: "concurrent writer did not affect any rows",
		},
		{
			name: "post transaction visibility",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioConcurrentWriter)
				record.Snapshot.PostCommitObservationSHA256 = record.Snapshot.ObservationBeforeSHA256
				return records
			},
			reason: "post-transaction observation does not prove the concurrent writer changed visible state",
		},
		{
			name: "session isolation",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioSessionIsolation)
				record.SessionIsolation.SessionAObservedBRows = 1
				return records
			},
			reason: "session-local evidence contains missing own rows or cross-session rows",
		},
		{
			name: "forced overflow receipt",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioForcedOverflow)
				events := record.Result.Stats.Samples[0].RuntimeReceiptEvents
				events[0].RuntimeIdentity = "wrong-fallback"
				return records
			},
			reason: "overflow receipt chain does not contain the exact configured fallback",
		},
		{
			name: "forced overflow cap expansion",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				record := operationalTestScenario(records, OperationalScenarioForcedOverflow)
				record.Result.Optimization.TargetOutcomes[0].StateLimit = record.PromotionIdentity.Caps["state_limit"] + 1
				return records
			},
			reason: "forced-overflow optimization cap is not a positive bounded variant of state_limit",
		},
		{
			name: "database identity",
			mutate: func(records []OperationalEvidenceRecord) []OperationalEvidenceRecord {
				records[len(records)-1].Result.PostgresEnvironment.DatabaseOID++
				return records
			},
			reason: "PostgreSQL database identity differs across operational records",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity := operationalTestPromotionIdentity()
			records := test.mutate(operationalTestEvidence(identity))
			report := buildOperationalGateReport(identity, operationalTestRequirements(), records)
			require.False(t, report.Passed)
			require.True(t, operationalTestReportContains(report, test.reason), "missing %q in report: global=%v records=%v", test.reason, report.Reasons, report.Records)
		})
	}
}

// TestOperationalGateRequirementsAreImmutable verifies callers cannot weaken
// the prescribed matrix, work_mem, cancellation, or fallback contract.
func TestOperationalGateRequirementsAreImmutable(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*OperationalGateRequirements)
		reason string
	}{
		{name: "pool", mutate: func(value *OperationalGateRequirements) { value.PoolSizes = []int{1} }, reason: "operational pool-size matrix must be exactly 1,2,8"},
		{name: "concurrency", mutate: func(value *OperationalGateRequirements) { value.ConcurrencyLevels = []int{1} }, reason: "operational concurrency matrix must be exactly 1,8,16"},
		{name: "cache", mutate: func(value *OperationalGateRequirements) { value.PlanCacheModes[2] = "off" }, reason: "operational plan-cache matrix must be exactly auto,force_custom_plan,force_generic_plan"},
		{name: "memory", mutate: func(value *OperationalGateRequirements) { value.LowWorkMemMaximumBytes = 128 * 1024 }, reason: "low work_mem ceiling must be positive and no greater than 64kB"},
		{name: "cancellation", mutate: func(value *OperationalGateRequirements) { value.CancellationMaximum = time.Second }, reason: "cancellation maximum must be positive and no greater than 250ms"},
		{name: "clean source", mutate: func(value *OperationalGateRequirements) { value.RequireCleanSource = false }, reason: "operational evidence must require a clean source tree"},
		{name: "candidate SQL", mutate: func(value *OperationalGateRequirements) { value.CandidateSQLFingerprint = "" }, reason: "operational candidate SQL fingerprint must be a canonical SHA-256 digest"},
		{name: "fallback", mutate: func(value *OperationalGateRequirements) { value.FallbackRuntimeIdentity = "other" }, reason: "fallback runtime identity differs from promotion fallback executor"},
		{name: "candidate mapping", mutate: func(value *OperationalGateRequirements) { value.CandidateRuntimeIdentity = operationalTestFallback }, reason: "candidate runtime identity differs from the registered promotion candidate mapping"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity := operationalTestPromotionIdentity()
			requirements := operationalTestRequirements()
			test.mutate(&requirements)
			report := buildOperationalGateReport(identity, requirements, operationalTestEvidence(identity))
			require.False(t, report.Passed)
			require.Contains(t, report.Reasons, test.reason)
		})
	}
}

// TestOperationalGateRequiresManifestSQLAnchor verifies the operational input
// cannot choose a self-consistent candidate SQL independently of promotion.
func TestOperationalGateRequiresManifestSQLAnchor(t *testing.T) {
	identity := operationalTestPromotionIdentity()
	requirements := operationalTestRequirements()
	requirements.CandidateSQLFingerprint = strings.Repeat("d", 64)
	report := buildOperationalGateReport(identity, requirements, operationalTestEvidence(identity))
	require.False(t, report.Passed)
	require.Contains(t, report.Reasons, "operational candidate SQL fingerprint differs from the promotion identity anchor")

	identity.OperationalCandidateSQLSHA256 = ""
	requirements.CandidateSQLFingerprint = ""
	report = buildOperationalGateReport(identity, requirements, operationalTestEvidence(identity))
	require.False(t, report.Passed)
	require.Contains(t, report.Reasons, "promotion identity operational candidate SQL SHA-256 must be a canonical digest")
}

// TestParsePostgresMemoryBytes verifies canonical PostgreSQL work_mem forms and invalid input.
func TestParsePostgresMemoryBytes(t *testing.T) {
	for value, expected := range map[string]int64{
		"64kB": 64 * 1024,
		"4MB":  4 * 1024 * 1024,
		"1GB":  1024 * 1024 * 1024,
		"512":  512,
	} {
		actual, err := parsePostgresMemoryBytes(value)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
	for _, value := range []string{"", "zero", "0MB", "1TB"} {
		_, err := parsePostgresMemoryBytes(value)
		require.Error(t, err)
	}
}

func operationalTestPromotionIdentity() PromotionEvidenceIdentity {
	digest := strings.Repeat("a", 64)
	return PromotionEvidenceIdentity{
		Candidate:                     operationalTestCandidate,
		SelectorVersion:               "sp-static-v8-hidden-fanin",
		ExecutionBoundary:             "guarded_dual_arm",
		FallbackExecutor:              operationalTestFallback,
		SourceCommit:                  "deadbeef",
		SourceSHA256:                  digest,
		BinarySHA256:                  strings.Repeat("b", 64),
		CorpusSHA256:                  strings.Repeat("c", 64),
		OperationalCandidateSQLSHA256: sqlFingerprint(operationalTestSQL),
		Caps:                          map[string]int64{"state_limit": 4096, "frontier_limit": 1024},
		Buckets: []PromotionBucket{{
			Name:                  "hidden-fanin",
			QuerySHA256:           []string{pgdriver.TraversalPolicyQuerySHA256(operationalTestCypher)},
			Direction:             "inbound",
			ObservationMode:       "distance",
			MinimumDepth:          1,
			MaximumDepth:          32,
			RelationshipKindCount: 1,
			QualificationSplit:    []string{"training", "holdout"},
		}},
	}
}

func operationalTestEvidence(identity PromotionEvidenceIdentity) []OperationalEvidenceRecord {
	var records []OperationalEvidenceRecord
	for _, poolSize := range defaultOperationalPoolSizes {
		for _, concurrency := range defaultOperationalConcurrency {
			for _, mode := range defaultOperationalPlanCacheModes {
				id := fmt.Sprintf("matrix-%s-p%d-c%d", mode, poolSize, concurrency)
				record := operationalTestRecord(identity, id, OperationalScenarioCandidateMatrix, mode, "4MB", poolSize)
				record.Concurrency = concurrency
				if poolSize != 1 {
					record.Result.Stats.Samples = []LatencySample{operationalTestPooledCandidateSample(record.Result)}
				}
				samples := make([]ConcurrencySample, 0, concurrency)
				connectionCount := poolSize
				if concurrency < connectionCount {
					connectionCount = concurrency
				}
				for worker := 1; worker <= concurrency; worker++ {
					connection := (worker-1)%connectionCount + 1
					classification := "warm-session"
					if worker <= connectionCount {
						classification = "cold-session"
					}
					samples = append(samples, ConcurrencySample{
						Worker: worker, Iteration: 1, ConnectionID: fmt.Sprint(100 + connection), Classification: classification,
						ExecuteDrain: 500 * time.Microsecond, Total: time.Millisecond,
					})
				}
				record.Result.Concurrency = []ConcurrencyBlock{{
					Concurrency: concurrency,
					PoolSize:    poolSize,
					Operations:  concurrency,
					Wall:        time.Millisecond,
					QPS:         float64(concurrency) * 1000,
					Samples:     samples,
				}}
				records = append(records, record)
			}
		}
	}

	lowMemory := operationalTestRecord(identity, "low-memory", OperationalScenarioLowWorkMem, "force_generic_plan", "64kB", 1)
	records = append(records, lowMemory)

	cancellation := operationalTestRecord(identity, "cancellation", OperationalScenarioCancellation, "auto", "4MB", 1)
	replay := operationalTestCandidateSample(cancellation.Result, "cancel-replay", "301")
	cancellation.Cancellation = &OperationalCancellationEvidence{
		SQLState:               "57014",
		Latency:                5 * time.Millisecond,
		TransactionRolledBack:  true,
		CancelledBackendPID:    301,
		ReplayBackendPID:       301,
		ReplaySucceeded:        true,
		ReplayCandidateReceipt: replay,
	}
	records = append(records, cancellation)

	snapshot := operationalTestRecord(identity, "snapshot", OperationalScenarioConcurrentWriter, "auto", "4MB", 2)
	snapshot.Snapshot = &OperationalSnapshotEvidence{
		ReaderBackendPID:            401,
		WriterBackendPID:            402,
		ReaderIsolation:             "repeatable read",
		WriterAffectedRows:          1,
		WriterCommitted:             true,
		ObservationBeforeSHA256:     strings.Repeat("e", 64),
		ObservationAfterSHA256:      strings.Repeat("e", 64),
		PostCommitObservationSHA256: strings.Repeat("f", 64),
	}
	records = append(records, snapshot)

	sessions := operationalTestRecord(identity, "sessions", OperationalScenarioSessionIsolation, "auto", "4MB", 2)
	sessionA := operationalTestCandidateSample(sessions.Result, "session-a", "501")
	sessionB := operationalTestCandidateSample(sessions.Result, "session-b", "502")
	sessions.SessionIsolation = &OperationalSessionIsolationEvidence{
		SessionABackendPID:       501,
		SessionBBackendPID:       502,
		SessionAInvocationID:     "session-a",
		SessionBInvocationID:     "session-b",
		SessionAOwnRows:          1,
		SessionBOwnRows:          1,
		SessionACandidateReceipt: sessionA,
		SessionBCandidateReceipt: sessionB,
	}
	records = append(records, sessions)

	overflow := operationalTestRecord(identity, "overflow", OperationalScenarioForcedOverflow, "force_custom_plan", "4MB", 1)
	overflow.Result.Optimization.TargetOutcomes[0].StateLimit = 1
	overflow.Result.Optimization.TargetOutcomes[0].FrontierLimit = 1
	overflow.Result.SQL = "select 1::int8 as distance /* forced overflow caps=1,1 */"
	overflow.Result.SQLFingerprint = sqlFingerprint(overflow.Result.SQL)
	overflow.Result.TraversalTelemetry = operationalTestTelemetry(identity, true)
	overflow.Result.Stats.Samples = []LatencySample{operationalTestFallbackSample(overflow.Result, "overflow-invocation", "601")}
	records = append(records, overflow)
	return records
}

func operationalTestBindRecordToIdentity(record *OperationalEvidenceRecord, identity PromotionEvidenceIdentity) {
	bucket := identity.Buckets[0]
	query := operationalTestCypher
	switch identity.Candidate {
	case string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness):
		query = "MATCH p = shortestPath((r)<-[:Traverse*1..64]-(e)) WHERE id(r) = $root_id AND id(e) = $end_id RETURN p"
	case string(optimize.ShortestPathExecutorI2GuardedDistance):
		query = "MATCH p = shortestPath((r)<-[:Traverse*1..64]-(e)) WHERE id(r) = $root_id AND id(e) = $end_id RETURN length(p)"
	case string(optimize.ShortestPathExecutorASPI1DAG):
		query = "MATCH p = allShortestPaths((s)-[:Traverse*1..64]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"
	case string(optimize.ExpansionSearchPolicyOrientationProbeV1), string(optimize.ExpansionSearchPolicyOrientationProbeV2):
		query = operationalTestOrientationCypher
	}
	record.PromotionIdentity = cloneOperationalPromotionIdentity(identity)
	record.SourceSHA256 = identity.SourceSHA256
	record.Result.Environment.SourceCommit = identity.SourceCommit
	record.Result.Environment.BinarySHA256 = identity.BinarySHA256
	record.Result.Environment.CorpusSHA256 = identity.CorpusSHA256
	record.Result.Cypher = query
	record.Result.SQL = operationalTestSQL
	record.Result.SQLFingerprint = sqlFingerprint(operationalTestSQL)
	record.Result.Shape.Direction = bucket.Direction
	record.Result.Shape.RelationshipKindCount = bucket.RelationshipKindCount
	record.Result.Shape.EdgeKinds = []string{"Traverse"}
	record.Result.Shape.MinDepth = operationalTestInt(bucket.MinimumDepth)
	record.Result.Shape.MaxDepth = operationalTestInt(bucket.MaximumDepth)
	record.Result.Shape.QualificationSplit = bucket.QualificationSplit[0]

	runtimeIdentity, _ := operationalCandidateRuntimeIdentity(identity.Candidate)
	fallback := record.Scenario == OperationalScenarioForcedOverflow
	record.Result.TraversalTelemetry = operationalTestIdentityTelemetry(identity, runtimeIdentity, fallback, bucket.ObservationMode)
	record.Result.Optimization = operationalTestIdentityOptimization(identity, bucket, fallback)
	if fallback {
		record.Result.SQL = operationalTestSQL + " /* forced overflow */"
		record.Result.SQLFingerprint = sqlFingerprint(record.Result.SQL)
		record.Result.Stats.Samples = []LatencySample{operationalTestIdentitySample(record.Result, runtimeIdentity, identity.FallbackExecutor, true, "overflow-invocation", "601")}
	} else if record.Scenario != OperationalScenarioCandidateMatrix || record.Result.Environment.PoolSize == 1 {
		record.Result.Stats.Samples = []LatencySample{operationalTestIdentitySample(record.Result, runtimeIdentity, runtimeIdentity, false, record.ID+"-invocation", "101")}
	} else {
		sample := operationalTestPooledCandidateSample(record.Result)
		sample.RequestedIdentity = runtimeIdentity
		sample.RuntimeIdentity = runtimeIdentity
		record.Result.Stats.Samples = []LatencySample{sample}
	}
	if record.Cancellation != nil {
		record.Cancellation.ReplayCandidateReceipt = operationalTestIdentitySample(record.Result, runtimeIdentity, runtimeIdentity, false, "cancel-replay", "301")
	}
	if record.SessionIsolation != nil {
		record.SessionIsolation.SessionACandidateReceipt = operationalTestIdentitySample(record.Result, runtimeIdentity, runtimeIdentity, false, "session-a", "501")
		record.SessionIsolation.SessionBCandidateReceipt = operationalTestIdentitySample(record.Result, runtimeIdentity, runtimeIdentity, false, "session-b", "502")
	}
}

func operationalTestIdentityOptimization(identity PromotionEvidenceIdentity, bucket PromotionBucket, forcedOverflow bool) *translate.OptimizationSummary {
	runtimeIdentity, _ := operationalCandidateRuntimeIdentity(identity.Candidate)
	eligible := true
	minimumDepth, maximumDepth := int64(bucket.MinimumDepth), int64(bucket.MaximumDepth)
	outcome := translate.TargetLoweringOutcome{
		Lowering: optimize.LoweringShortestPathExecutor, TargetKind: "traversal", Family: "SP",
		Candidate: identity.Candidate, Selected: identity.Candidate, Applied: identity.Candidate,
		Fallback: identity.FallbackExecutor, EmittedPolicy: operationalCandidatePolicy(identity.Candidate),
		PlannedCandidates: []string{identity.FallbackExecutor, identity.Candidate},
		EmittedCandidates: []string{identity.Candidate, identity.FallbackExecutor},
		ExecutionBoundary: identity.ExecutionBoundary, SelectorVersion: identity.SelectorVersion,
		SelectionMode: "production_canary", ObservationMode: bucket.ObservationMode, Direction: bucket.Direction,
		RelationshipKindCount: bucket.RelationshipKindCount, UntypedRelationship: bucket.UntypedRelationship,
		Eligible: &eligible, StaticallyEligible: &eligible, MinimumDepth: &minimumDepth, MaximumDepth: &maximumDepth,
	}
	if identity.Candidate == string(optimize.ShortestPathExecutorASPI1DAG) {
		outcome.Family = "ASP"
	}
	for name, value := range identity.Caps {
		if forcedOverflow {
			value = 1
		}
		switch name {
		case "state_limit":
			outcome.StateLimit = value
		case "frontier_limit":
			outcome.FrontierLimit = value
		case "predecessor_limit":
			outcome.PredecessorLimit = value
		case "enumeration_limit":
			outcome.EnumerationLimit = value
		case "output_bytes_limit":
			outcome.OutputBytesLimit = value
		}
	}
	if isOrientationProbePolicy(identity.Candidate) {
		forward := string(optimize.ExpansionSearchStepwiseForward)
		reverse := string(optimize.ExpansionSearchSuffixSeededReverse)
		outcome.Lowering = optimize.LoweringExpansionSearchStrategy
		outcome.Family = "fixed_suffix_expansion"
		outcome.Candidate, outcome.Selected, outcome.Applied, outcome.Fallback = reverse, forward, forward, forward
		outcome.EmittedPolicy = identity.Candidate
		outcome.PlannedCandidates = []string{forward, reverse}
		outcome.EmittedCandidates = []string{forward, reverse}
		outcome.Direction = ""
		outcome.EligibilityFacts = []translate.TargetEligibilityFact{{Name: "qualified_fixed_suffix_topology", Eligible: true}}
		capValue := func(name string) int64 {
			value := identity.Caps[name]
			if forcedOverflow {
				return 1
			}
			return value
		}
		outcome.ProbeCaps = &optimize.ExpansionSearchProbeCaps{
			RootRowLimit: capValue("root_row_limit"), ReverseSeedRowLimit: capValue("reverse_seed_row_limit"),
			DirectionalDegreeRowLimit: capValue("directional_degree_row_limit"),
		}
		outcome.Admission = &optimize.ExpansionSearchAdmission{
			StateLimit: capValue("state_limit"), RequiresCompleteProbes: true, FallbackStrategy: optimize.ExpansionSearchStepwiseForward,
		}
		outcome.StateLimit = outcome.Admission.StateLimit
		_ = runtimeIdentity
	}
	return &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}}
}

func operationalTestIdentityTelemetry(identity PromotionEvidenceIdentity, runtimeIdentity string, fallback bool, observationMode string) *TraversalExecutionTelemetry {
	telemetry := operationalTestTelemetry(identity, fallback && identity.Candidate == string(optimize.ShortestPathExecutorI2GuardedDistance))
	summary := &telemetry.Summary
	summary.RequestedIdentity = runtimeIdentity
	summary.PlannedIdentities = []string{runtimeIdentity, identity.FallbackExecutor}
	summary.EmittedIdentity = operationalCandidatePolicy(identity.Candidate)
	summary.RuntimeIdentity = runtimeIdentity
	summary.AppliedIdentity = runtimeIdentity
	summary.SelectorVersion = identity.SelectorVersion
	summary.ExecutionBoundary = identity.ExecutionBoundary
	summary.ObservationMode = observationMode
	summary.RuntimeBranch = "selected_candidate"
	if identity.Candidate == string(optimize.ShortestPathExecutorI2GuardedDistance) {
		summary.RuntimeBranch = "inline_canonical_distance"
	}
	summary.FallbackIdentity = ""
	summary.FallbackExecuted = operationalTestBool(false)
	summary.Overflow = operationalTestBool(false)
	if fallback {
		summary.RuntimeIdentity = identity.FallbackExecutor
		summary.AppliedIdentity = identity.FallbackExecutor
		summary.RuntimeBranch = "exact_configured_fallback"
		if identity.Candidate == string(optimize.ShortestPathExecutorI2GuardedDistance) {
			summary.RuntimeBranch = "exact_s4_distance_fallback"
		}
		summary.FallbackIdentity = identity.FallbackExecutor
		summary.Provenance["fallback_identity"] = "test"
		summary.FallbackExecuted = operationalTestBool(true)
		summary.Overflow = operationalTestBool(true)
	}
	if identity.Candidate != string(optimize.ShortestPathExecutorI2GuardedDistance) {
		telemetry.Level = TraversalTelemetryLevelSummary
		telemetry.Diagnostic = nil
	}
	return telemetry
}

func operationalTestIdentitySample(result CaseResult, requested, runtime string, fallback bool, invocation, connection string) LatencySample {
	sample := operationalTestCandidateSample(result, invocation, connection)
	sample.RequestedIdentity = requested
	sample.RuntimeIdentity = runtime
	sample.RuntimeBranch = "selected_candidate"
	sample.FallbackExecuted = operationalTestBool(fallback)
	sample.RuntimeReceiptEvents = []RuntimeReceiptEvent{{InvocationID: invocation, Ordinal: 1, RuntimeIdentity: runtime, RuntimeBranch: sample.RuntimeBranch, FallbackExecuted: fallback}}
	if fallback {
		sample.RuntimeBranch = "exact_configured_fallback"
		sample.RuntimeReceiptEvents[0].RuntimeBranch = sample.RuntimeBranch
	}
	return sample
}

func operationalTestInt(value int) *int { return &value }

func operationalTestRecord(identity PromotionEvidenceIdentity, id string, scenario OperationalEvidenceScenario, planCacheMode, workMem string, poolSize int) OperationalEvidenceRecord {
	minimumDepth, maximumDepth := 1, 32
	sql := operationalTestSQL
	result := CaseResult{
		Environment: &RunEnvironment{
			ArtifactSchemaVersion: 2,
			CorpusSHA256:          identity.CorpusSHA256,
			SourceCommit:          identity.SourceCommit,
			DirtyDiffSHA256:       cleanWorkingTreeSHA256(),
			BinarySHA256:          identity.BinarySHA256,
			PoolSize:              poolSize,
		},
		PostgresEnvironment: &PostgresEnvironment{
			Version:              "PostgreSQL 17",
			Database:             "operational",
			PlanCacheMode:        planCacheMode,
			TransactionIsolation: "repeatable read",
			WorkMem:              workMem,
			TempFileLimit:        "-1",
			GraphPartitionCount:  1,
			PostmasterStartedAt:  time.Unix(1_700_000_000, 0).UTC(),
			DatabaseOID:          42,
			Autovacuum:           "on",
			SchemaFingerprint:    strings.Repeat("1", 64),
			IndexFingerprint:     strings.Repeat("2", 64),
		},
		Fixture: &FixtureMetadata{
			Dataset: "operational-dataset", Checksum: strings.Repeat("4", 64),
			NodeCount: 2, EdgeCount: 1, PhysicalValidated: true, PhysicalNodeCount: 2, PhysicalEdgeCount: 1,
			Configuration: "generated-shortest-operational-v1",
		},
		Source:         "generated_sp_i2_distance_v1.json",
		Dataset:        "operational-dataset",
		Name:           "operational-case",
		WorkloadSHA256: strings.Repeat("3", 64),
		Category:       "generated_shortest_path_v2",
		Shape: WorkloadShape{
			QualificationSplit: "training", RootPredicate: "bound_id", TerminalPredicate: "bound_id",
			EdgeKinds: []string{"Traverse"}, Direction: "inbound", RelationshipKindCount: 1,
			MinDepth: &minimumDepth, MaxDepth: &maximumDepth,
		},
		ExecutionMode:      ModePostgresSQL,
		Status:             StatusOK,
		Cypher:             operationalTestCypher,
		Params:             map[string]any{"root_id": int64(101), "end_id": int64(202)},
		NodeParams:         map[string]string{"root_id": "root", "end_id": "end"},
		NodeListParams:     map[string][]string{"targets": {"end"}},
		SQL:                sql,
		SQLFingerprint:     sqlFingerprint(sql),
		StableObservation:  true,
		RowCount:           1,
		ObservedRows:       []string{"[1]"},
		TraversalTelemetry: operationalTestTelemetry(identity, false),
		Optimization:       operationalTestOptimization(identity),
	}
	result.Stats = DurationStats{
		Iterations: 1,
		Median:     time.Millisecond,
		Samples:    []LatencySample{operationalTestCandidateSample(result, id+"-invocation", "101")},
	}
	return OperationalEvidenceRecord{
		ID:                id,
		Scenario:          scenario,
		PromotionIdentity: cloneOperationalPromotionIdentity(identity),
		SourceSHA256:      identity.SourceSHA256,
		Result:            result,
	}
}

func operationalTestOptimization(identity PromotionEvidenceIdentity) *translate.OptimizationSummary {
	minimumDepth, maximumDepth := int64(1), int64(32)
	eligible := true
	return &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
		Lowering: optimize.LoweringShortestPathExecutor, TargetKind: "traversal", Family: "SP",
		Candidate: identity.Candidate, Selected: identity.Candidate, Applied: identity.Candidate,
		Fallback: identity.FallbackExecutor, EmittedPolicy: optimize.ShortestPathPolicyI2DistanceGuardedV1,
		PlannedCandidates: []string{identity.FallbackExecutor, identity.Candidate},
		EmittedCandidates: []string{identity.Candidate, identity.FallbackExecutor},
		ExecutionBoundary: identity.ExecutionBoundary, SelectorVersion: identity.SelectorVersion,
		SelectionMode: "production_canary", ObservationMode: "distance", Direction: "inbound",
		RelationshipKindCount: 1, Eligible: &eligible, StaticallyEligible: &eligible,
		MinimumDepth: &minimumDepth, MaximumDepth: &maximumDepth,
		StateLimit: identity.Caps["state_limit"], FrontierLimit: identity.Caps["frontier_limit"],
	}}}
}

func operationalTestTelemetry(identity PromotionEvidenceIdentity, fallback bool) *TraversalExecutionTelemetry {
	runtimeIdentity := operationalTestCandidate
	runtimeBranch := "inline_canonical_distance"
	applied := operationalTestCandidate
	overflow := false
	provenance := map[string]string{
		"requested_identity":        "test",
		"planned_identities":        "test",
		"emitted_identity":          "test",
		"runtime_identity":          "test",
		"applied_identity":          "test",
		"selector_version":          "test",
		"scheduler_version":         "test",
		"runtime_branch":            "test",
		"runtime_outcome_available": "test",
		"observation_mode":          "test",
		"overflow":                  "test",
		"fallback_executed":         "test",
		"caps.state_rows":           "test",
	}
	fallbackIdentity := ""
	if fallback {
		runtimeIdentity = operationalTestFallback
		runtimeBranch = "exact_s4_distance_fallback"
		applied = operationalTestFallback
		overflow = true
		fallbackIdentity = operationalTestFallback
		provenance["fallback_identity"] = "test"
	}
	available := true
	stateLimit := identity.Caps["state_limit"]
	frontierLimit := identity.Caps["frontier_limit"]
	stateRows := int64(2)
	outputRows := int64(1)
	candidateMarkerRows := int64(1)
	fallbackMarkerRows := int64(0)
	candidateBranchRows := int64(1)
	fallbackBranchRows := int64(0)
	candidateExecutorLoops := int64(1)
	fallbackExecutorLoops := int64(0)
	if fallback {
		stateLimit = 1
		frontierLimit = 1
		candidateMarkerRows = 0
		fallbackMarkerRows = 1
		candidateBranchRows = 0
		fallbackBranchRows = 1
		candidateExecutorLoops = 0
		fallbackExecutorLoops = 1
	}
	provenance["caps.state_rows"] = "test"
	provenance["caps.frontier_rows"] = "test"
	planCounters := map[string]int64{
		"sp_i2_distance_rows":            stateRows,
		"sp_i2_target_rows":              candidateBranchRows,
		"sp_i2_output_rows":              outputRows,
		"sp_i2_candidate_marker_rows":    candidateMarkerRows,
		"sp_i2_fallback_marker_rows":     fallbackMarkerRows,
		"sp_i2_candidate_branch_rows":    candidateBranchRows,
		"sp_i2_fallback_branch_rows":     fallbackBranchRows,
		"sp_i2_candidate_executor_loops": candidateExecutorLoops,
		"sp_i2_fallback_executor_loops":  fallbackExecutorLoops,
	}
	diagnosticProvenance := map[string]string{}
	planProvenance := map[string]string{}
	for _, name := range []string{
		"state_rows", "frontier_rows", "output_rows", "candidate_marker_rows", "fallback_marker_rows",
		"candidate_branch_rows", "fallback_branch_rows", "candidate_executor_loops", "fallback_executor_loops",
	} {
		diagnosticProvenance["inline_shortest_distance."+name] = "test"
	}
	for name := range planCounters {
		planProvenance["counters."+name] = "test"
	}
	timedSample := false
	return &TraversalExecutionTelemetry{
		SchemaVersion: TraversalExecutionTelemetrySchemaVersion,
		Level:         TraversalTelemetryLevelDiagnostic,
		Summary: TraversalExecutionSummary{
			RequestedIdentity:       operationalTestCandidate,
			PlannedIdentities:       []string{operationalTestCandidate, operationalTestFallback},
			EmittedIdentity:         "sp-i2-distance-guarded-v1",
			RuntimeIdentity:         runtimeIdentity,
			AppliedIdentity:         applied,
			SelectorVersion:         identity.SelectorVersion,
			SchedulerVersion:        "single_ended_level",
			ExecutionBoundary:       identity.ExecutionBoundary,
			ObservationMode:         "distance",
			Caps:                    map[string]int64{"state_rows": stateLimit, "frontier_rows": frontierLimit},
			RuntimeOutcomeAvailable: &available,
			RuntimeBranch:           runtimeBranch,
			Overflow:                operationalTestBool(overflow),
			FallbackExecuted:        operationalTestBool(fallback),
			FallbackIdentity:        fallbackIdentity,
			Provenance:              provenance,
		},
		Diagnostic: &TraversalExecutionDiagnostic{
			InvocationID:     "operational-diagnostic",
			ConnectionID:     "operational-diagnostic-connection",
			TimedSample:      &timedSample,
			RequiredFamilies: []TraversalTelemetryFamily{TraversalTelemetryFamilySP},
			Counters: TraversalDiagnosticCounters{InlineShortestDistance: &InlineDistanceTraversalCounters{
				StateRows: &stateRows, FrontierRows: &stateRows, OutputRows: &outputRows,
				CandidateMarkerRows: &candidateMarkerRows, FallbackMarkerRows: &fallbackMarkerRows,
				CandidateBranchRows: &candidateBranchRows, FallbackBranchRows: &fallbackBranchRows,
				CandidateExecutorLoops: &candidateExecutorLoops, FallbackExecutorLoops: &fallbackExecutorLoops,
			}},
			CounterStatus: TraversalTelemetryCounterStatusComplete,
			PlanReplay: &TraversalPlanReplayEvidence{
				Source: "postgres_explain_analyze_timing_off", Counters: planCounters, Provenance: planProvenance,
			},
			Provenance: diagnosticProvenance,
		},
	}
}

func operationalTestCandidateSample(result CaseResult, invocation, connection string) LatencySample {
	fallback := false
	return LatencySample{
		Round: 1, Iteration: 1, Case: result.Name, Dataset: result.Dataset, Backend: ModePostgresSQL,
		ConnectionID: connection, Classification: "warm", Duration: time.Millisecond,
		RequestedIdentity: operationalTestCandidate, RuntimeIdentity: operationalTestCandidate,
		RuntimeBranch: "inline_canonical_distance", FallbackExecuted: &fallback,
		RuntimeAttestation: "timed_invocation", RuntimeInvocationID: invocation,
		RuntimeReceiptEvents: []RuntimeReceiptEvent{{
			InvocationID: invocation, Ordinal: 1, RuntimeIdentity: operationalTestCandidate,
			RuntimeBranch: "inline_canonical_distance", FallbackExecuted: false,
		}},
	}
}

func operationalTestPooledCandidateSample(result CaseResult) LatencySample {
	fallback := false
	return LatencySample{
		Round: 1, Iteration: 1, Case: result.Name, Dataset: result.Dataset, Backend: ModePostgresSQL,
		Classification: "warm", Duration: time.Millisecond,
		RequestedIdentity: operationalTestCandidate, RuntimeIdentity: operationalTestCandidate,
		RuntimeBranch: "inline_canonical_distance", FallbackExecuted: &fallback,
		RuntimeAttestation: "same_case_invocation_local_replay",
	}
}

func operationalTestFallbackSample(result CaseResult, invocation, connection string) LatencySample {
	fallback := true
	return LatencySample{
		Round: 1, Iteration: 1, Case: result.Name, Dataset: result.Dataset, Backend: ModePostgresSQL,
		ConnectionID: connection, Classification: "warm", Duration: time.Millisecond,
		RequestedIdentity: operationalTestCandidate, RuntimeIdentity: operationalTestTerminal,
		RuntimeBranch: "exact_relationship_trail_fallback", FallbackExecuted: &fallback,
		RuntimeAttestation: "timed_invocation", RuntimeInvocationID: invocation,
		RuntimeReceiptEvents: []RuntimeReceiptEvent{
			{InvocationID: invocation, Ordinal: 1, RuntimeIdentity: operationalTestFallback, RuntimeBranch: "exact_s4_distance_fallback", FallbackExecuted: true},
			{InvocationID: invocation, Ordinal: 2, RuntimeIdentity: operationalTestTerminal, RuntimeBranch: "exact_relationship_trail_fallback", FallbackExecuted: true},
		},
	}
}

func operationalTestBool(value bool) *bool {
	return &value
}

func operationalTestScenario(records []OperationalEvidenceRecord, scenario OperationalEvidenceScenario) *OperationalEvidenceRecord {
	for index := range records {
		if records[index].Scenario == scenario {
			return &records[index]
		}
	}
	panic("scenario not found: " + string(scenario))
}

func operationalTestReportContains(report OperationalGateReport, reason string) bool {
	for _, actual := range report.Reasons {
		if strings.Contains(actual, reason) {
			return true
		}
	}
	for _, record := range report.Records {
		for _, actual := range record.Reasons {
			if strings.Contains(actual, reason) {
				return true
			}
		}
	}
	return false
}

func operationalTestWriteJSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o600))
}

func operationalTestCloneReport(t *testing.T, report OperationalGateReport) OperationalGateReport {
	t.Helper()
	raw, err := json.Marshal(report)
	require.NoError(t, err)
	var clone OperationalGateReport
	require.NoError(t, json.Unmarshal(raw, &clone))
	return clone
}
