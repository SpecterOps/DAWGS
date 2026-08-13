// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

// TestBuildPerfGateReportTreatsNeo4jAsCorrectnessOracle verifies that PostgreSQL receives latency ratios while Neo4j contributes correctness observations without performance gating.
func TestBuildPerfGateReportTreatsNeo4jAsCorrectnessOracle(t *testing.T) {
	baseline := []CaseResult{
		perfGateRecord("one_shortest_path_bound_pair", ModePostgresSQL, 10*time.Millisecond, 5, 30),
		perfGateRecord("one_shortest_path_bound_pair", ModeNeo4j, 3*time.Millisecond, 5, 30),
	}
	candidate := []CaseResult{
		perfGateRecord("one_shortest_path_bound_pair", ModePostgresSQL, 3*time.Millisecond, 5, 30),
		perfGateRecord("one_shortest_path_bound_pair", ModeNeo4j, 2*time.Millisecond, 5, 30),
	}

	report, err := buildPerfGateReport(baseline, candidate, qualifiedPerfGateOptions(t, baseline, candidate, PerfGateOptions{
		Seed:                42,
		Confidence:          0.95,
		RegressionThreshold: 0.20,
		BootstrapCount:      250,
	}))

	require.NoError(t, err)
	require.True(t, report.Passed)
	require.Len(t, report.Cases, 2)
	postgres := findPerfGateCase(t, report.Cases, ModePostgresSQL)
	require.InDelta(t, 0.3, postgres.MedianRatio.Estimate, 0.0001)
	require.NotNil(t, postgres.P95Ratio)
	neo4j := findPerfGateCase(t, report.Cases, ModeNeo4j)
	require.True(t, neo4j.OracleOnly)
	require.Nil(t, neo4j.P95Ratio)
}

// TestBuildPerfGateReportFailsMissingDeclaredPostgresCase verifies that every declared PostgreSQL workload must have a candidate record and that the declaration set is fingerprinted.
func TestBuildPerfGateReportFailsMissingDeclaredPostgresCase(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("present", ModePostgresSQL, time.Millisecond, 5, 30)}
	candidate := []CaseResult{perfGateRecord("present", ModePostgresSQL, time.Millisecond, 5, 30)}

	report, err := buildPerfGateReport(baseline, candidate, qualifiedPerfGateOptions(t, baseline, candidate, PerfGateOptions{
		Seed:                1,
		Confidence:          0.95,
		RegressionThreshold: 0.20,
		BootstrapCount:      100,
		DeclaredBackends: []DeclaredCaseBackend{
			{
				Dataset: "fixture",
				Name:    "present",
				Backend: ModePostgresSQL,
			},
			{
				Dataset: "fixture",
				Name:    "missing",
				Backend: ModePostgresSQL,
			},
		},
	}))

	require.NoError(t, err)
	require.False(t, report.Passed)
	require.NotEmpty(t, report.DeclarationSHA256)
	var missing PerfGateCase
	for _, gateCase := range report.Cases {
		if gateCase.Name == "missing" {
			missing = gateCase
		}
	}
	require.Equal(t, "missing", missing.CandidateStatus)
	require.ErrorContains(t, reasonsError(missing.Reasons), "required candidate record status is missing")
}

// TestBuildPerfGateReportAppliesMaterialityOnlyToDeclaredTargets verifies that a named target passes only when the confidence-bound saving clears both ratio and absolute thresholds.
func TestBuildPerfGateReportAppliesMaterialityOnlyToDeclaredTargets(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("target", ModePostgresSQL, 10*time.Millisecond, 5, 30)}
	candidate := []CaseResult{perfGateRecord("target", ModePostgresSQL, 9_700*time.Microsecond, 5, 30)}

	report, err := buildPerfGateReport(baseline, candidate, qualifiedPerfGateOptions(t, baseline, candidate, PerfGateOptions{
		Seed:                1,
		Confidence:          0.95,
		RegressionThreshold: 0.20,
		BootstrapCount:      100,
		TargetNames:         []string{"target"},
		MaterialityRatio:    0.95,
		MaterialityAbsolute: 100 * time.Microsecond,
	}))

	require.NoError(t, err)
	require.True(t, report.Passed, "%v", report.Cases[0].Reasons)
	require.NotNil(t, report.Cases[0].MedianSaving)
	require.Equal(t, 300*time.Microsecond, report.Cases[0].MedianSaving.Lower)
}

// TestBuildPerfGateReportFailsRegressionAndInsufficientP95 verifies that an excessive median slowdown and fewer than 150 warm samples independently fail a PostgreSQL gate case.
func TestBuildPerfGateReportFailsRegressionAndInsufficientP95(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, 10*time.Millisecond, 5, 10)}
	candidate := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, 13*time.Millisecond, 5, 10)}

	report, err := buildPerfGateReport(baseline, candidate, qualifiedPerfGateOptions(t, baseline, candidate, PerfGateOptions{
		Seed:                7,
		Confidence:          0.95,
		RegressionThreshold: 0.20,
		BootstrapCount:      100,
	}))

	require.NoError(t, err)
	require.False(t, report.Passed)
	require.Len(t, report.Cases, 1)
	require.ErrorContains(t, reasonsError(report.Cases[0].Reasons), "median regression")
	require.ErrorContains(t, reasonsError(report.Cases[0].Reasons), "at least 150 warm samples")
}

// TestBuildPerfGateReportRequiresMatchedRounds verifies that four baseline/candidate rounds are insufficient for an inferential gate even with ample samples.
func TestBuildPerfGateReportRequiresMatchedRounds(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, 10*time.Millisecond, 4, 40)}
	candidate := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, 9*time.Millisecond, 4, 40)}

	report, err := buildPerfGateReport(baseline, candidate, qualifiedPerfGateOptions(t, baseline, candidate, PerfGateOptions{
		Seed:                1,
		Confidence:          0.95,
		RegressionThreshold: 0.20,
		BootstrapCount:      100,
	}))

	require.NoError(t, err)
	require.False(t, report.Passed)
	require.ErrorContains(t, reasonsError(report.Cases[0].Reasons), "at least 5 matched rounds")
}

// TestBuildPerfGateReportRequiresHostAAEvidence verifies that a non-diagnostic promotion cannot substitute fixed defaults for a checksummed host calibration.
func TestBuildPerfGateReportRequiresHostAAEvidence(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, time.Millisecond, 5, 30)}
	candidate := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, time.Millisecond, 5, 30)}
	stampPairedEvidence(baseline, candidate, minimumDiscoveryWarmups)

	_, err := buildPerfGateReport(baseline, candidate, PerfGateOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 10,
	})

	require.ErrorContains(t, err, "checksummed host A/A report")
}

// TestBuildPerfGateReportRequiresMaterialityTargetForPromotion verifies a
// containment-only comparison can pass without authorizing a no-win rollout.
func TestBuildPerfGateReportRequiresMaterialityTargetForPromotion(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, time.Millisecond, 5, 30)}
	candidate := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, time.Millisecond, 5, 30)}
	report, err := buildPerfGateReport(baseline, candidate, qualifiedPerfGateOptions(t, baseline, candidate, PerfGateOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 10,
	}))

	require.NoError(t, err)
	require.True(t, report.Passed)
	require.True(t, report.MaterialityRequired)
	require.False(t, report.MaterialityPassed)
	require.False(t, report.PromotionEligible)
}

// TestBuildPerfGateReportRejectsMismatchedAAHost verifies a syntactically valid calibration from another host cannot qualify production timing.
func TestBuildPerfGateReportRejectsMismatchedAAHost(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, time.Millisecond, 5, 30)}
	candidate := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, time.Millisecond, 5, 30)}
	options := qualifiedPerfGateOptions(t, baseline, candidate, PerfGateOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 10,
	})
	options.AAReport.HostFingerprint = strings.Repeat("c", 64)

	_, err := buildPerfGateReport(baseline, candidate, options)

	require.ErrorContains(t, err, "host fingerprint does not match")
}

// TestBuildPerfGateReportUsesP95AbsoluteFloor verifies a relative regression below 100us remains inside the mandatory fast-case floor while preserving the absolute interval in the report.
func TestBuildPerfGateReportUsesP95AbsoluteFloor(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("fast", ModePostgresSQL, time.Millisecond, 5, 30)}
	candidate := []CaseResult{perfGateRecord("fast", ModePostgresSQL, 1060*time.Microsecond, 5, 30)}

	report, err := buildPerfGateReport(baseline, candidate, qualifiedPerfGateOptions(t, baseline, candidate, PerfGateOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 100,
	}))

	require.NoError(t, err)
	require.True(t, report.Passed, "%v", report.Cases[0].Reasons)
	require.Equal(t, minimumTimingNoiseAbsolute, report.Cases[0].P95NoiseAbsolute)
	require.Equal(t, 60*time.Microsecond, report.Cases[0].P95Change.Lower)
}

// TestBuildPerfGateReportRejectsUnbalancedPromotionEvidence verifies matched rounds with one fixed arm order cannot support promotion.
func TestBuildPerfGateReportRejectsUnbalancedPromotionEvidence(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, time.Millisecond, 5, 30)}
	candidate := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, 900*time.Microsecond, 5, 30)}
	options := qualifiedPerfGateOptions(t, baseline, candidate, PerfGateOptions{Confidence: defaultConfidenceLevel, BootstrapCount: 10})
	for idx := range baseline[0].Stats.Samples {
		baseline[0].Stats.Samples[idx].ArmOrder = 1
		candidate[0].Stats.Samples[idx].ArmOrder = 2
	}

	_, err := buildPerfGateReport(baseline, candidate, options)

	require.ErrorContains(t, err, "arm order is not balanced")
}

// TestBuildPerfGateReportKeepsStressTimingDiagnostic verifies stress latency cannot fail production timing gates even without A/A or paired-order evidence.
func TestBuildPerfGateReportKeepsStressTimingDiagnostic(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("stress", ModePostgresSQL, time.Millisecond, 1, 1)}
	candidate := []CaseResult{perfGateRecord("stress", ModePostgresSQL, 10*time.Millisecond, 1, 1)}
	baseline[0].Shape.FixtureTier = "stress"
	candidate[0].Shape.FixtureTier = "stress"

	report, err := buildPerfGateReport(baseline, candidate, PerfGateOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 10,
	})

	require.NoError(t, err)
	require.True(t, report.Passed)
	require.False(t, report.Cases[0].TimingGated)
	require.Contains(t, report.Cases[0].Reasons, "stress tier timing is diagnostic")
}

// TestBuildPerfGateReportKeepsDiagnosticSplitOutOfPromotion verifies a normal
// fixture explicitly reserved for boundary diagnostics needs no A/A evidence
// and cannot make the report promotion eligible.
func TestBuildPerfGateReportKeepsDiagnosticSplitOutOfPromotion(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("boundary", ModePostgresSQL, time.Millisecond, 1, 1)}
	candidate := []CaseResult{perfGateRecord("boundary", ModePostgresSQL, 10*time.Millisecond, 1, 1)}
	baseline[0].Shape.QualificationSplit = "diagnostic"
	candidate[0].Shape.QualificationSplit = "diagnostic"

	report, err := buildPerfGateReport(baseline, candidate, PerfGateOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 10,
	})

	require.NoError(t, err)
	require.True(t, report.Passed)
	require.False(t, report.PromotionEligible)
	require.False(t, report.Cases[0].TimingGated)
	require.Contains(t, report.Cases[0].Reasons, "diagnostic qualification split is excluded from promotion timing")
}

// TestBuildPerfGateReportRejectsChangedLogicalWorkload verifies that baseline and candidate records with different workload digests cannot be compared.
func TestBuildPerfGateReportRejectsChangedLogicalWorkload(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, 10*time.Millisecond, 5, 30)}
	candidate := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, 9*time.Millisecond, 5, 30)}
	candidate[0].WorkloadSHA256 = "changed-workload"

	_, err := buildPerfGateReport(baseline, candidate, PerfGateOptions{
		Seed:                1,
		Confidence:          0.95,
		RegressionThreshold: 0.20,
		BootstrapCount:      100,
	})
	require.ErrorContains(t, err, "logical workload differs")
}

// TestUnsupportedDeclarationAffectsChecksumWithoutRequiringARecord verifies that an explicitly unsupported backend needs no measurement but its reason remains part of declaration identity.
func TestUnsupportedDeclarationAffectsChecksumWithoutRequiringARecord(t *testing.T) {
	declared := []DeclaredCaseBackend{
		{
			Dataset: "fixture",
			Name:    "directionless",
			Backend: ModeNeo4j,
		},
		{
			Dataset:           "fixture",
			Name:              "directionless",
			Backend:           ModePostgresSQL,
			UnsupportedReason: "unsupported form",
		},
	}
	records := []CaseResult{perfGateRecord("directionless", ModeNeo4j, time.Millisecond, 1, 1)}

	report, err := buildPerfGateReport(records, records, PerfGateOptions{
		Seed:                1,
		Confidence:          0.95,
		RegressionThreshold: 0.20,
		BootstrapCount:      10,
		DeclaredBackends:    declared,
	})
	require.NoError(t, err)
	require.True(t, report.Passed)
	require.Len(t, report.Cases, 1)

	changed := append([]DeclaredCaseBackend(nil), declared...)
	changed[1].UnsupportedReason = "different reason"
	require.NotEqual(t, declarationSHA256(declared), declarationSHA256(changed))
}

// TestValidatePerformanceArtifactSelectionsRefusesDiagnosticsFromCompleteGate verifies that subset artifacts require an explicit diagnostic override and still must share the same declaration digest.
func TestValidatePerformanceArtifactSelectionsRefusesDiagnosticsFromCompleteGate(t *testing.T) {
	manifest := &SelectionManifest{
		DiagnosticOnly:    true,
		DeclarationSHA256: "subset",
	}
	left := []CaseResult{{
		Dataset: "fixture",
		Name:    "case",
		Environment: &RunEnvironment{
			Selection: manifest,
		},
	}}
	right := []CaseResult{{
		Dataset: "fixture",
		Name:    "case",
		Environment: &RunEnvironment{
			Selection: manifest,
		},
	}}

	require.ErrorContains(t, validatePerformanceArtifactSelections(left, right, false), "refused")
	require.NoError(t, validatePerformanceArtifactSelections(left, right, true))
	right[0].Environment.Selection = &SelectionManifest{
		DiagnosticOnly:    true,
		DeclarationSHA256: "different",
	}
	require.ErrorContains(t, validatePerformanceArtifactSelections(left, right, true), "declarations differ")
}

// perfGateRecord returns one successful workload observation with identical warm samples arranged into the requested rounds.
func perfGateRecord(name string, mode ExecutionMode, duration time.Duration, rounds, samplesPerRound int) CaseResult {
	record := CaseResult{
		Dataset:        "fixture",
		Name:           name,
		WorkloadSHA256: fmt.Sprintf("workload:%s:%s", name, mode),
		ExecutionMode:  mode,
		Status:         StatusOK,
		Shape:          WorkloadShape{FixtureTier: "normal"},
		Environment: &RunEnvironment{
			GOOS: "linux", GOARCH: "amd64", CPUCount: 8, CPUModel: "test-cpu", Kernel: "test-kernel", CgroupCPU: "max 100000",
			WarmupIterations: minimumDiscoveryWarmups,
		},
	}
	record.Stats.WarmupIterations = minimumDiscoveryWarmups
	for round := 1; round <= rounds; round++ {
		for iteration := 1; iteration <= samplesPerRound; iteration++ {
			record.Stats.Samples = append(record.Stats.Samples, LatencySample{
				Round:          round,
				Iteration:      iteration,
				Classification: "warm",
				Duration:       duration,
			})
		}
	}
	return record
}

// qualifiedPerfGateOptions stamps balanced pairing metadata and supplies host-matched A/A evidence.
func qualifiedPerfGateOptions(t *testing.T, baseline, candidate []CaseResult, options PerfGateOptions) PerfGateOptions {
	t.Helper()
	stampPairedEvidence(baseline, candidate, minimumDiscoveryWarmups)
	options.AAReport = testAAReportForRecords(t, baseline)
	options.AAReportSHA256 = strings.Repeat("b", 64)
	return options
}

func testAAReportForRecords(t *testing.T, records []CaseResult) *AAResolutionReport {
	t.Helper()
	hostFingerprint, err := artifactHostFingerprint(records)
	require.NoError(t, err)

	keys := map[performanceKey]struct{}{}
	for _, record := range records {
		if record.ExecutionMode == ModePostgresSQL && hasWarmLatencySample(record) {
			keys[performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}] = struct{}{}
		}
	}
	aa := &AAResolutionReport{
		Version:                      aaReportVersion,
		Confidence:                   defaultConfidenceLevel,
		ArtifactSHA256:               strings.Repeat("a", 64),
		HostFingerprint:              hostFingerprint,
		MinimumRounds:                minimumGateRounds,
		MinimumSamplesPerArmPerRound: 10,
		OrderBalanced:                true,
	}
	for _, key := range sortedPerformanceKeys(keys) {
		workloadSHA256, err := workloadSHA256ForKey(records, key)
		require.NoError(t, err)
		aa.Cases = append(aa.Cases, AAResolutionCase{
			Dataset: key.dataset, Name: key.name, Backend: key.backend, WorkloadSHA256: workloadSHA256, Rounds: minimumGateRounds, SamplesPerArm: minimumGateRounds * 10,
			P50: testAAMetricResolution(), P95: testAAMetricResolution(),
		})
	}
	return aa
}

func testAAMetricResolution() AAMetricResolution {
	return AAMetricResolution{
		Ratio:              RatioInterval{Estimate: 1, Lower: 0.99, Upper: 1.01},
		RatioResolution:    0.01,
		AbsoluteChange:     DurationInterval{Estimate: 0, Lower: -10 * time.Microsecond, Upper: 10 * time.Microsecond},
		AbsoluteResolution: 10 * time.Microsecond,
	}
}

func stampPairedEvidence(left, right []CaseResult, warmups int) {
	stamp := func(records []CaseResult, arm string, leftArm bool) {
		for recordIdx := range records {
			record := &records[recordIdx]
			record.Stats.WarmupIterations = warmups
			if record.Environment == nil {
				record.Environment = &RunEnvironment{}
			}
			record.Environment.WarmupIterations = warmups
			record.Environment.Arm = arm
			for sampleIdx := range record.Stats.Samples {
				sample := &record.Stats.Samples[sampleIdx]
				leftFirst := sample.Round%2 == 1
				order := 2
				if leftArm == leftFirst {
					order = 1
				}
				sample.Block = sample.Round
				sample.Arm = arm
				sample.ArmOrder = order
				sample.RunUUID = fmt.Sprintf("pair-%s-%d", record.Name, sample.Round)
			}
		}
	}
	stamp(left, "baseline", true)
	stamp(right, "candidate", false)
}

// findPerfGateCase returns the report entry for a backend or fails the calling test when the gate omitted it.
func findPerfGateCase(t *testing.T, cases []PerfGateCase, mode ExecutionMode) PerfGateCase {
	t.Helper()
	for _, gateCase := range cases {
		if gateCase.Backend == mode {
			return gateCase
		}
	}
	t.Fatalf("missing %s gate case", mode)
	return PerfGateCase{}
}

// reasonsError joins gate-failure reasons into one diagnostic error.
func reasonsError(reasons []string) error {
	return fmt.Errorf("%s", strings.Join(reasons, "; "))
}

// TestQualificationSplitFailsClosedOnMissingOrDriftingTraversalPartitions
// verifies benchmark artifacts cannot silently reclassify selector training as
// frozen holdout evidence.
func TestQualificationSplitFailsClosedOnMissingOrDriftingTraversalPartitions(t *testing.T) {
	key := performanceKey{dataset: "fixture", name: "sp", backend: ModePostgresSQL}
	left := []CaseResult{{
		Dataset: "fixture", Name: "sp", Category: "generated_shortest_path_v2", ExecutionMode: ModePostgresSQL,
	}}
	_, err := qualificationSplit(key, left)
	require.ErrorContains(t, err, "no frozen qualification split")

	left[0].Shape.QualificationSplit = "training"
	right := append([]CaseResult(nil), left...)
	right[0].Shape.QualificationSplit = "holdout"
	_, err = qualificationSplit(key, left, right)
	require.ErrorContains(t, err, "changes qualification split")

	right[0].Shape.QualificationSplit = "training"
	split, err := qualificationSplit(key, left, right)
	require.NoError(t, err)
	require.Equal(t, "training", split)
}

// TestQualificationSplitRecognizesCompatibleFixedSuffixV2Categories verifies
// the v2 dataset cannot bypass partition enforcement through its intentionally
// backwards-compatible category name.
func TestQualificationSplitRecognizesCompatibleFixedSuffixV2Categories(t *testing.T) {
	key := performanceKey{dataset: "generated_fixed_suffix_expansion_v2_d8_f16", name: "GFSE-V2-D08-F016", backend: ModePostgresSQL}
	records := []CaseResult{{
		Dataset: key.dataset, Name: key.name, Category: "generated_fixed_suffix_expansion", ExecutionMode: key.backend,
	}}

	_, err := qualificationSplit(key, records)
	require.ErrorContains(t, err, "no frozen qualification split")
}

// TestBuildPerfGateReportRequiresIndependentTraversalHoldout verifies a
// complete release gate cannot be assembled from selector-training topology
// alone even when every measured case passes.
func TestBuildPerfGateReportRequiresIndependentTraversalHoldout(t *testing.T) {
	baseline := []CaseResult{
		perfGateRecord("sp-training", ModePostgresSQL, 10*time.Millisecond, minimumGateRounds, 30),
		perfGateRecord("sp-holdout", ModePostgresSQL, 10*time.Millisecond, minimumGateRounds, 30),
	}
	candidate := []CaseResult{
		perfGateRecord("sp-training", ModePostgresSQL, 5*time.Millisecond, minimumGateRounds, 30),
		perfGateRecord("sp-holdout", ModePostgresSQL, 5*time.Millisecond, minimumGateRounds, 30),
	}
	for _, records := range [][]CaseResult{baseline, candidate} {
		records[0].Category = "generated_shortest_path_v2"
		records[0].Shape.QualificationSplit = "training"
		records[1].Category = "generated_shortest_path_v2"
		records[1].Shape.QualificationSplit = "holdout"
	}

	report, err := buildPerfGateReport(baseline, candidate, qualifiedPerfGateOptions(t, baseline, candidate, PerfGateOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 50, TargetNames: []string{"sp-training", "sp-holdout"},
	}))
	require.NoError(t, err)
	require.True(t, report.QualificationRequired)
	require.True(t, report.TrainingPassed)
	require.True(t, report.HoldoutPassed)
	require.True(t, report.QualificationPassed)
	require.True(t, report.Passed)
	require.True(t, report.PromotionEligible)
	require.Equal(t, []TraversalQualificationStatus{{
		Family: "SP", TrainingCases: 1, HoldoutCases: 1, TrainingPassed: true, HoldoutPassed: true, Passed: true,
	}}, report.QualificationFamilies)

	// A passing ASP holdout may not qualify an SP candidate's training data.
	baseline[1].Cypher = "RETURN allShortestPaths((a)-[:E*1..3]->(b))"
	candidate[1].Cypher = baseline[1].Cypher
	report, err = buildPerfGateReport(baseline, candidate, qualifiedPerfGateOptions(t, baseline, candidate, PerfGateOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 50, TargetNames: []string{"sp-training", "sp-holdout"},
	}))
	require.NoError(t, err)
	require.False(t, report.QualificationPassed)
	require.False(t, report.Passed)
	require.False(t, report.PromotionEligible)
	baseline[1].Cypher = ""
	candidate[1].Cypher = ""

	for idx := range baseline {
		baseline[idx].Optimization = &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{
			{TargetKind: "traversal", Family: "SP", Applied: "SP-S4-C-D", Selected: "SP-S4-C-D"},
			{TargetKind: "endpoint_resolution", Family: "endpoint_resolution", TraversalFamily: "SP", Applied: "ENDPOINT-RESOLUTION-INCUMBENT"},
		}}
		candidate[idx].Optimization = &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{
			{TargetKind: "traversal", Family: "SP", Applied: "SP-B1-C-ALT-NODE-D", Selected: "SP-B1-C-ALT-NODE-D"},
			{TargetKind: "endpoint_resolution", Family: "endpoint_resolution", TraversalFamily: "SP", Applied: "ENDPOINT-RESOLUTION-INCUMBENT"},
		}}
		fallback := false
		available := true
		candidate[idx].TraversalTelemetry = &TraversalExecutionTelemetry{Summary: TraversalExecutionSummary{
			RequestedIdentity: "SP-B1-C-ALT-NODE-D", RuntimeIdentity: "SP-B1-C-ALT-NODE-D",
			RuntimeBranch: "bidirectional_search", RuntimeOutcomeAvailable: &available, FallbackExecuted: &fallback,
		}}
		setSampleTraversalRuntimeMetadata(&candidate[idx].Stats, candidate[idx].TraversalTelemetry)
		for sampleIdx := range candidate[idx].Stats.Samples {
			candidate[idx].Stats.Samples[sampleIdx].RuntimeAttestation = "timed_invocation"
			candidate[idx].Stats.Samples[sampleIdx].RuntimeReceiptEvents = []RuntimeReceiptEvent{{
				Ordinal: 1, RuntimeIdentity: "SP-B1-C-ALT-NODE-D", RuntimeBranch: "bidirectional_search", FallbackExecuted: false,
			}}
		}
	}
	report, err = buildPerfGateReport(baseline, candidate, qualifiedPerfGateOptions(t, baseline, candidate, PerfGateOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 50, TargetNames: []string{"sp-training", "sp-holdout"},
	}))
	require.NoError(t, err)
	require.True(t, report.QualificationPassed)
	require.Equal(t, "SP-B1-C-ALT-NODE-D@bidirectional_search", report.QualificationFamilies[0].Family)
	candidate[0].Stats.Samples[0].RuntimeAttestation = "same_case_invocation_local_replay"
	require.ErrorContains(t, validateCandidateRuntimeEvidence(candidate, performanceKey{
		dataset: candidate[0].Dataset, name: candidate[0].Name, backend: candidate[0].ExecutionMode,
	}), "runtime attribution")
	candidate[0].Stats.Samples[0].RuntimeAttestation = "timed_invocation"
	for idx := range baseline {
		baseline[idx].Optimization = nil
		candidate[idx].Optimization = nil
	}

	baseline = baseline[:1]
	candidate = candidate[:1]
	report, err = buildPerfGateReport(baseline, candidate, qualifiedPerfGateOptions(t, baseline, candidate, PerfGateOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 50, TargetNames: []string{"sp-training"},
	}))
	require.NoError(t, err)
	require.True(t, report.TrainingPassed)
	require.False(t, report.HoldoutPassed)
	require.False(t, report.QualificationPassed)
	require.False(t, report.Passed)
	require.False(t, report.PromotionEligible)
}

func TestValidateRuntimeReceiptEventsPreservesNestedFallbackChain(t *testing.T) {
	fallback := true
	events := []RuntimeReceiptEvent{
		{Ordinal: 1, RuntimeIdentity: "SP-I1-C-WE+MAT-M0", RuntimeBranch: "candidate_overflow", FallbackExecuted: true},
		{Ordinal: 2, RuntimeIdentity: "SP-S4-C-WE+MAT-M0", RuntimeBranch: "workspace_overflow", FallbackExecuted: true},
		{Ordinal: 3, RuntimeIdentity: "SP-S3-U-E+MAT-M0", RuntimeBranch: "exact_fallback", FallbackExecuted: true},
	}
	require.NoError(t, validateRuntimeReceiptEvents(events, "SP-S3-U-E+MAT-M0", "exact_fallback", &fallback))

	events[1].Ordinal = 3
	require.ErrorContains(t, validateRuntimeReceiptEvents(events, "SP-S3-U-E+MAT-M0", "exact_fallback", &fallback), "not contiguous")
}
