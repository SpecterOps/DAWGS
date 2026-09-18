// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/stretchr/testify/require"
)

// TestOrientationSelectorV2ReportPassesForwardAndReverseWithApplicableShadowGate verifies orientation selector v2 report passes forward and reverse with applicable shadow gate behavior.
func TestOrientationSelectorV2ReportPassesForwardAndReverseWithApplicableShadowGate(t *testing.T) {
	artifacts := orientationSelectorV2Artifacts{}
	for index := range 8 {
		training := orientationSelectorV2Records(
			"training", string(optimize.ExpansionSearchStepwiseForward),
			10*time.Millisecond+50*time.Microsecond, 10*time.Millisecond, 14*time.Millisecond, 10*time.Millisecond+60*time.Microsecond,
			false,
		)
		renameOrientationV2Records(fmt.Sprintf("training-forward-%02d", index), training)
		artifacts = appendOrientationV2Artifacts(artifacts, training)
	}
	for index := range 4 {
		holdout := orientationSelectorV2Records(
			"holdout", string(optimize.ExpansionSearchSuffixSeededReverse),
			30*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond, 5*time.Millisecond+50*time.Microsecond,
			false,
		)
		renameOrientationV2Records(fmt.Sprintf("holdout-reverse-%02d", index), holdout)
		artifacts = appendOrientationV2Artifacts(artifacts, holdout)
	}

	report, err := buildOrientationSelectorV2Report(
		artifacts.shadow, artifacts.incumbent, artifacts.reverse, artifacts.guarded,
		testAAReportForRecords(t, artifacts.incumbent),
		OrientationSelectorV2ReportOptions{
			Seed:           7,
			Confidence:     defaultConfidenceLevel,
			BootstrapCount: 100,
			Protocol:       referencePairProtocolDiscovery,
		},
	)

	require.NoError(t, err)
	require.Equal(t, orientationSelectorReportV2Version, report.Version)
	require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV2), report.Policy)
	require.False(t, report.QualificationPassed)
	require.Zero(t, report.TrainingCases)
	require.Zero(t, report.HoldoutCases)
	require.Len(t, report.Cases, 12)
	for _, entry := range report.Cases {
		require.True(t, entry.Passed)
		require.True(t, entry.GuardedSelectedOverhead.Passed)
		require.True(t, entry.GuardedFastestRegret.Passed)
		if entry.WouldSelectIdentity == string(optimize.ExpansionSearchStepwiseForward) {
			require.True(t, entry.ShadowForwardOverhead.Applicable)
		} else {
			require.False(t, entry.ShadowForwardOverhead.Applicable)
			require.Greater(t, entry.ShadowForwardOverhead.Ratio.Upper, 1.10)
			require.False(t, entry.ShadowForwardOverhead.Passed)
		}
	}
}

// TestOrientationSelectorV2ConfirmationBindsCanonicalCohortAndFrozenDiscovery verifies orientation selector v2 confirmation binds canonical cohort and frozen discovery behavior.
func TestOrientationSelectorV2ConfirmationBindsCanonicalCohortAndFrozenDiscovery(t *testing.T) {
	training, full := canonicalOrientationV2TestArtifacts(t)
	discovery, err := buildOrientationSelectorV2Report(
		training.shadow, training.incumbent, training.reverse, training.guarded,
		testAAReportForRecords(t, training.incumbent),
		OrientationSelectorV2ReportOptions{
			Seed:           5,
			Confidence:     defaultConfidenceLevel,
			BootstrapCount: 50,
			Protocol:       referencePairProtocolDiscovery,
		},
	)
	require.NoError(t, err)
	discovery.ShadowArtifactSHA256, discovery.IncumbentArtifactSHA256 = testSHA("1"), testSHA("2")
	discovery.ReverseArtifactSHA256, discovery.GuardedArtifactSHA256 = testSHA("3"), testSHA("4")
	discovery.AAReportSHA256 = testSHA("5")
	canonical, err := canonicalOrientationV2Cohort()
	require.NoError(t, err)
	freeze := testOrientationV2Freeze()
	freeze.DirtyDiffSHA256 = cleanWorkingTreeSHA256()
	freeze.CohortDeclarationSHA256 = canonical.declarationSHA256

	report, err := buildOrientationSelectorV2Report(
		full.shadow, full.incumbent, full.reverse, full.guarded,
		testAAReportForRecords(t, full.incumbent),
		OrientationSelectorV2ReportOptions{
			Seed:           7,
			Confidence:     defaultConfidenceLevel,
			BootstrapCount: 50,
			Protocol:       referencePairProtocolConfirmation,
			Freeze:         freeze,
			Discovery:      &discovery,
		},
	)

	require.NoError(t, err)
	require.True(t, report.QualificationPassed)
	require.Equal(t, 8, report.TrainingCases)
	require.Equal(t, 4, report.HoldoutCases)
	require.Equal(t, canonical.declarationSHA256, report.CohortDeclarationSHA256)
}

// TestCreateOrientationSelectorV2DiscoveryWritesBoundFreeze verifies create orientation selector v2 discovery writes bound freeze behavior.
func TestCreateOrientationSelectorV2DiscoveryWritesBoundFreeze(t *testing.T) {
	training, _ := canonicalOrientationV2TestArtifacts(t)
	training = compactOrientationV2Artifacts(training, 5, 10)
	directory := t.TempDir()
	paths := map[string]string{
		"shadow": filepath.Join(directory, "shadow.jsonl"), "incumbent": filepath.Join(directory, "incumbent.jsonl"),
		"reverse": filepath.Join(directory, "reverse.jsonl"), "guarded": filepath.Join(directory, "guarded.jsonl"),
		"aa": filepath.Join(directory, "aa.json"), "report": filepath.Join(directory, "discovery.json"),
		"freeze": filepath.Join(directory, "freeze.json"),
	}
	writeOrientationV2TestArtifact(t, paths["shadow"], training.shadow)
	writeOrientationV2TestArtifact(t, paths["incumbent"], training.incumbent)
	writeOrientationV2TestArtifact(t, paths["reverse"], training.reverse)
	writeOrientationV2TestArtifact(t, paths["guarded"], training.guarded)
	require.NoError(t, writeAAResolutionReport(paths["aa"], *testAAReportForRecords(t, training.incumbent)))

	passed, err := createOrientationSelectorV2Report(
		paths["shadow"], paths["incumbent"], paths["reverse"], paths["guarded"], paths["aa"], "", "", paths["freeze"], paths["report"],
		OrientationSelectorV2ReportOptions{
			Seed:           11,
			Confidence:     defaultConfidenceLevel,
			BootstrapCount: 10,
			Protocol:       referencePairProtocolDiscovery,
		},
	)

	require.NoError(t, err)
	require.False(t, passed)
	freeze, _, err := loadOrientationSelectorV2FreezeManifest(paths["freeze"])
	require.NoError(t, err)
	report, err := loadOrientationSelectorV2Report(paths["report"])
	require.NoError(t, err)
	reportSHA256, err := fileSHA256(paths["report"])
	require.NoError(t, err)
	canonical, err := canonicalOrientationV2Cohort()
	require.NoError(t, err)
	require.Equal(t, reportSHA256, freeze.DiscoveryReportSHA256)
	require.Equal(t, canonical.declarationSHA256, freeze.CohortDeclarationSHA256)
	require.Equal(t, report.Policy, freeze.Policy)
	require.Equal(t, cleanWorkingTreeSHA256(), freeze.DirtyDiffSHA256)
}

// TestOrientationSelectorV2ReportEnforcesEachLatencyGate verifies orientation selector v2 report enforces each latency gate behavior.
func TestOrientationSelectorV2ReportEnforcesEachLatencyGate(t *testing.T) {
	for _, testCase := range []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// choice retains the choice while anonymous record is assembled or evaluated.
		choice string
		// shadow retains the shadow while anonymous record is assembled or evaluated.
		shadow time.Duration
		// forward retains the forward while anonymous record is assembled or evaluated.
		forward time.Duration
		// reverse retains the reverse while anonymous record is assembled or evaluated.
		reverse time.Duration
		// guarded retains the guarded while anonymous record is assembled or evaluated.
		guarded time.Duration
		// reason retains the reason while anonymous record is assembled or evaluated.
		reason string
		// shadowFails indicates whether shadow fails applies.
		shadowFails bool
		// selectedFails indicates whether selected fails applies.
		selectedFails bool
		// fastestFails indicates whether fastest fails applies.
		fastestFails bool
	}{
		{
			name:        "forward shadow",
			choice:      string(optimize.ExpansionSearchStepwiseForward),
			shadow:      12 * time.Millisecond,
			forward:     10 * time.Millisecond,
			reverse:     14 * time.Millisecond,
			guarded:     10 * time.Millisecond,
			reason:      "forward-selected shadow overhead",
			shadowFails: true,
		},
		{
			name:          "guarded selected",
			choice:        string(optimize.ExpansionSearchSuffixSeededReverse),
			shadow:        20 * time.Millisecond,
			forward:       10 * time.Millisecond,
			reverse:       5 * time.Millisecond,
			guarded:       7 * time.Millisecond,
			reason:        "guarded selected-arm overhead",
			selectedFails: true,
			fastestFails:  true,
		},
		{
			name:         "guarded fastest",
			choice:       string(optimize.ExpansionSearchStepwiseForward),
			shadow:       10 * time.Millisecond,
			forward:      10 * time.Millisecond,
			reverse:      5 * time.Millisecond,
			guarded:      10 * time.Millisecond,
			reason:       "guarded fastest-arm regret",
			fastestFails: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			artifacts := orientationSelectorV2Records("training", testCase.choice, testCase.shadow, testCase.forward, testCase.reverse, testCase.guarded, false)
			report, err := buildOrientationSelectorV2Report(
				artifacts.shadow, artifacts.incumbent, artifacts.reverse, artifacts.guarded,
				testAAReportForRecords(t, artifacts.incumbent),
				OrientationSelectorV2ReportOptions{
					Seed:           11,
					Confidence:     defaultConfidenceLevel,
					BootstrapCount: 100,
					Protocol:       referencePairProtocolDiscovery,
				},
			)
			require.NoError(t, err)
			require.False(t, report.Cases[0].Passed)
			require.Contains(t, report.Cases[0].Reasons[0]+fmt.Sprint(report.Cases[0].Reasons[1:]), testCase.reason)
			require.Equal(t, testCase.shadowFails, report.Cases[0].ShadowForwardOverhead.Applicable && !report.Cases[0].ShadowForwardOverhead.Passed)
			require.Equal(t, testCase.selectedFails, !report.Cases[0].GuardedSelectedOverhead.Passed)
			require.Equal(t, testCase.fastestFails, !report.Cases[0].GuardedFastestRegret.Passed)
		})
	}
}

// TestOrientationSelectorV2ReportAcceptsExactOverflowFallback verifies orientation selector v2 report accepts exact overflow fallback behavior.
func TestOrientationSelectorV2ReportAcceptsExactOverflowFallback(t *testing.T) {
	artifacts := orientationSelectorV2Records(
		"training", string(optimize.ExpansionSearchStepwiseForward),
		10*time.Millisecond, 10*time.Millisecond, 12*time.Millisecond, 10*time.Millisecond,
		true,
	)
	report, err := buildOrientationSelectorV2Report(
		artifacts.shadow, artifacts.incumbent, artifacts.reverse, artifacts.guarded,
		testAAReportForRecords(t, artifacts.incumbent),
		OrientationSelectorV2ReportOptions{
			Seed:           13,
			Confidence:     defaultConfidenceLevel,
			BootstrapCount: 50,
			Protocol:       referencePairProtocolDiscovery,
		},
	)
	require.NoError(t, err)
	require.True(t, report.Cases[0].Overflow)
	require.True(t, report.Cases[0].FallbackExecuted)
	require.Equal(t, "exact_forward_incumbent", report.Cases[0].GuardedRuntimeBranch)
}

// TestOrientationSelectorV2ReportAcceptsStateOverflowAfterReverseChoice verifies orientation selector v2 report accepts state overflow after reverse choice behavior.
func TestOrientationSelectorV2ReportAcceptsStateOverflowAfterReverseChoice(t *testing.T) {
	artifacts := orientationSelectorV2Records(
		"training", string(optimize.ExpansionSearchSuffixSeededReverse),
		10*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond, 10*time.Millisecond,
		true,
	)
	for index := range artifacts.shadow {
		artifacts.shadow[index].TraversalTelemetry.Summary.Overflow = boolPointer(false)
	}
	report, err := buildOrientationSelectorV2Report(
		artifacts.shadow, artifacts.incumbent, artifacts.reverse, artifacts.guarded,
		testAAReportForRecords(t, artifacts.incumbent),
		OrientationSelectorV2ReportOptions{
			Seed:           17,
			Confidence:     defaultConfidenceLevel,
			BootstrapCount: 50,
			Protocol:       referencePairProtocolDiscovery,
		},
	)
	require.NoError(t, err)
	require.True(t, report.Cases[0].Overflow)
	require.True(t, report.Cases[0].FallbackExecuted)
	require.Equal(t, string(optimize.ExpansionSearchStepwiseForward), report.Cases[0].GuardedRuntimeIdentity)
}

// TestOrientationSelectorV2ReportRejectsIncompleteConfirmationCohort verifies orientation selector v2 report rejects incomplete confirmation cohort behavior.
func TestOrientationSelectorV2ReportRejectsIncompleteConfirmationCohort(t *testing.T) {
	artifacts := orientationSelectorV2Records(
		"training", string(optimize.ExpansionSearchStepwiseForward),
		10*time.Millisecond, 10*time.Millisecond, 12*time.Millisecond, 10*time.Millisecond, false,
	)
	renameOrientationV2Records("training-only", artifacts)
	_, err := buildOrientationSelectorV2Report(
		artifacts.shadow, artifacts.incumbent, artifacts.reverse, artifacts.guarded,
		testAAReportForRecords(t, artifacts.incumbent),
		OrientationSelectorV2ReportOptions{
			Confidence:     defaultConfidenceLevel,
			BootstrapCount: 10,
			Protocol:       referencePairProtocolConfirmation,
			Freeze:         testOrientationV2Freeze(),
		},
	)
	require.ErrorContains(t, err, "exact frozen 8-training/4-holdout cohort")
}

// TestOrientationSelectorV2ReportRequiresFrozenDiscoveryForConfirmation verifies orientation selector v2 report requires frozen discovery for confirmation behavior.
func TestOrientationSelectorV2ReportRequiresFrozenDiscoveryForConfirmation(t *testing.T) {
	artifacts := orientationSelectorV2Records(
		"training", string(optimize.ExpansionSearchStepwiseForward),
		10*time.Millisecond, 10*time.Millisecond, 12*time.Millisecond, 10*time.Millisecond, false,
	)
	_, err := buildOrientationSelectorV2Report(
		artifacts.shadow, artifacts.incumbent, artifacts.reverse, artifacts.guarded,
		testAAReportForRecords(t, artifacts.incumbent),
		OrientationSelectorV2ReportOptions{
			Confidence:     defaultConfidenceLevel,
			BootstrapCount: 10,
			Protocol:       referencePairProtocolConfirmation,
		},
	)
	require.Error(t, err)
}

// TestOrientationSelectorV2ReportRejectsRuntimeIdentityAndReceiptDrift verifies orientation selector v2 report rejects runtime identity and receipt drift behavior.
func TestOrientationSelectorV2ReportRejectsRuntimeIdentityAndReceiptDrift(t *testing.T) {
	for _, mutate := range []func(*orientationSelectorV2Artifacts){
		func(artifacts *orientationSelectorV2Artifacts) {
			artifacts.guarded[0].TraversalTelemetry.Summary.RuntimeIdentity = string(optimize.ExpansionSearchStepwiseForward)
			artifacts.guarded[0].TraversalTelemetry.Summary.AppliedIdentity = string(optimize.ExpansionSearchStepwiseForward)
			artifacts.guarded[0].TraversalTelemetry.Summary.RuntimeBranch = "exact_forward_incumbent"
		},
		func(artifacts *orientationSelectorV2Artifacts) {
			artifacts.guarded[0].TraversalTelemetry.Summary.FallbackExecuted = boolPointer(true)
			artifacts.guarded[0].TraversalTelemetry.Summary.FallbackIdentity = string(optimize.ExpansionSearchStepwiseForward)
		},
		func(artifacts *orientationSelectorV2Artifacts) {
			artifacts.guarded[0].Stats.Samples[1].RuntimeReceiptEvents = nil
		},
		func(artifacts *orientationSelectorV2Artifacts) {
			artifacts.shadow[0].TraversalTelemetry.Summary.EmittedIdentity = string(optimize.ExpansionSearchPolicyOrientationProbeV1)
		},
		func(artifacts *orientationSelectorV2Artifacts) {
			artifacts.incumbent[0].TraversalTelemetry.Summary.SelectorVersion = "static-lowering-v1"
		},
		func(artifacts *orientationSelectorV2Artifacts) {
			artifacts.guarded[0].TraversalTelemetry.Summary.ExecutionBoundary = optimize.ExpansionSearchExecutionBoundaryInlineStatement
		},
	} {
		artifacts := orientationSelectorV2Records(
			"training", string(optimize.ExpansionSearchSuffixSeededReverse),
			10*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond, 5*time.Millisecond, false,
		)
		mutate(&artifacts)
		_, err := buildOrientationSelectorV2Report(
			artifacts.shadow, artifacts.incumbent, artifacts.reverse, artifacts.guarded,
			testAAReportForRecords(t, artifacts.incumbent),
			OrientationSelectorV2ReportOptions{
				Confidence:     defaultConfidenceLevel,
				BootstrapCount: 10,
				Protocol:       referencePairProtocolDiscovery,
			},
		)
		require.Error(t, err)
	}
}

// TestOrientationSelectorV2ReportRejectsIdentityCaseObservationAndOrderDrift verifies orientation selector v2 report rejects identity case observation and order drift behavior.
func TestOrientationSelectorV2ReportRejectsIdentityCaseObservationAndOrderDrift(t *testing.T) {
	mutations := []func(*orientationSelectorV2Artifacts){
		func(artifacts *orientationSelectorV2Artifacts) {
			artifacts.guarded[0].Environment.CorpusSHA256 = testSHA("9")
		},
		func(artifacts *orientationSelectorV2Artifacts) { artifacts.reverse[0].WorkloadSHA256 = "changed" },
		func(artifacts *orientationSelectorV2Artifacts) {
			artifacts.guarded[0].ObservedRows = []string{"changed"}
		},
		func(artifacts *orientationSelectorV2Artifacts) { artifacts.guarded[0].SQLFingerprint = "changed" },
		func(artifacts *orientationSelectorV2Artifacts) { artifacts.guarded = artifacts.guarded[1:] },
		func(artifacts *orientationSelectorV2Artifacts) {
			for idx := range artifacts.guarded {
				artifacts.guarded[idx].Name = "unexpected"
			}
		},
		func(artifacts *orientationSelectorV2Artifacts) {
			artifacts.reverse[0].Shape.QualificationSplit = "holdout"
		},
		func(artifacts *orientationSelectorV2Artifacts) { artifacts.guarded[0].Fixture.Checksum = "changed" },
		func(artifacts *orientationSelectorV2Artifacts) {
			artifacts.reverse[0].PostgresEnvironment.EdgeRelationBytes++
		},
		func(artifacts *orientationSelectorV2Artifacts) {
			artifacts.shadow[0].PostgresEnvironment.AnalyzeState = "edge:never"
		},
		func(artifacts *orientationSelectorV2Artifacts) {
			for sampleIdx := range artifacts.guarded[0].Stats.Samples {
				artifacts.guarded[0].Stats.Samples[sampleIdx].ArmOrder = 3
			}
		},
	}
	for _, mutate := range mutations {
		artifacts := orientationSelectorV2Records(
			"training", string(optimize.ExpansionSearchSuffixSeededReverse),
			10*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond, 5*time.Millisecond, false,
		)
		mutate(&artifacts)
		_, err := buildOrientationSelectorV2Report(
			artifacts.shadow, artifacts.incumbent, artifacts.reverse, artifacts.guarded,
			testAAReportForRecords(t, artifacts.incumbent),
			OrientationSelectorV2ReportOptions{
				Confidence:     defaultConfidenceLevel,
				BootstrapCount: 10,
				Protocol:       referencePairProtocolDiscovery,
			},
		)
		require.Error(t, err)
	}
}

// TestOrientationSelectorV2ReportRejectsUnboundAAEnvironment verifies orientation selector v2 report rejects unbound aa environment behavior.
func TestOrientationSelectorV2ReportRejectsUnboundAAEnvironment(t *testing.T) {
	artifacts := orientationSelectorV2Records(
		"training", string(optimize.ExpansionSearchStepwiseForward),
		10*time.Millisecond, 10*time.Millisecond, 12*time.Millisecond, 10*time.Millisecond, false,
	)
	for _, mutate := range []func(*AAResolutionReport){
		func(report *AAResolutionReport) { report.Cases[0].PostgresEnvironmentSHA256 = "" },
		func(report *AAResolutionReport) { report.Cases[0].FixtureSHA256 = testSHA("9") },
	} {
		aa := testAAReportForRecords(t, artifacts.incumbent)
		mutate(aa)
		_, err := buildOrientationSelectorV2Report(
			artifacts.shadow, artifacts.incumbent, artifacts.reverse, artifacts.guarded, aa,
			OrientationSelectorV2ReportOptions{
				Confidence:     defaultConfidenceLevel,
				BootstrapCount: 10,
				Protocol:       referencePairProtocolDiscovery,
			},
		)
		require.ErrorContains(t, err, "incumbent A/A environment")
	}
}

// TestOrientationSelectorV2ReportRejectsSupplementalMeasurements verifies orientation selector v2 report rejects supplemental measurements behavior.
func TestOrientationSelectorV2ReportRejectsSupplementalMeasurements(t *testing.T) {
	mutations := []func(*CaseResult){
		func(record *CaseResult) { record.Concurrency = []ConcurrencyBlock{{Concurrency: 2}} },
		func(record *CaseResult) { record.PostgresReferences = []PostgresReferenceResult{{Name: "unexpected"}} },
		func(record *CaseResult) { record.ClientWaterfall = &ClientWaterfall{} },
		func(record *CaseResult) { record.RawPGXWaterfall = &PostgresBoundaryWaterfall{} },
		func(record *CaseResult) { record.RawPGXRoundTrip = &PostgresBoundaryWaterfall{} },
	}
	for _, mutate := range mutations {
		artifacts := orientationSelectorV2Records(
			"training", string(optimize.ExpansionSearchStepwiseForward),
			10*time.Millisecond, 10*time.Millisecond, 12*time.Millisecond, 10*time.Millisecond, false,
		)
		mutate(&artifacts.shadow[0])
		_, err := buildOrientationSelectorV2Report(
			artifacts.shadow, artifacts.incumbent, artifacts.reverse, artifacts.guarded,
			testAAReportForRecords(t, artifacts.incumbent),
			OrientationSelectorV2ReportOptions{
				Confidence:     defaultConfidenceLevel,
				BootstrapCount: 10,
				Protocol:       referencePairProtocolDiscovery,
			},
		)
		require.ErrorContains(t, err, "mixes selector timing with supplemental PostgreSQL measurements")
	}
}

// orientationSelectorV2Artifacts groups state that must remain consistent while processing orientation selector v2 artifacts.
type orientationSelectorV2Artifacts struct {
	// shadow retains the shadow while orientationSelectorV2Artifacts is assembled or evaluated.
	shadow []CaseResult
	// incumbent retains the incumbent while orientationSelectorV2Artifacts is assembled or evaluated.
	incumbent []CaseResult
	// reverse retains the reverse while orientationSelectorV2Artifacts is assembled or evaluated.
	reverse []CaseResult
	// guarded retains the guarded while orientationSelectorV2Artifacts is assembled or evaluated.
	guarded []CaseResult
}

// canonicalOrientationV2TestArtifacts builds a complete frozen artifact set for selector-v2 tests.
func canonicalOrientationV2TestArtifacts(t *testing.T) (orientationSelectorV2Artifacts, orientationSelectorV2Artifacts) {
	t.Helper()
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	full := orientationSelectorV2Artifacts{}
	for _, testCase := range corpus.Cases {
		isTraining := false
		if !slices.Contains(testCase.Tags, "orientation-v2-training") && !slices.Contains(testCase.Tags, "orientation-v2-holdout") {
			continue
		}
		isTraining = testCase.Shape.QualificationSplit == "training"
		choice := string(optimize.ExpansionSearchSuffixSeededReverse)
		shadow, forward, reverse, guarded := 20*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond, 5*time.Millisecond+50*time.Microsecond
		if isTraining {
			choice = string(optimize.ExpansionSearchStepwiseForward)
			shadow, forward, reverse, guarded = 10*time.Millisecond+50*time.Microsecond, 10*time.Millisecond, 14*time.Millisecond, 10*time.Millisecond+60*time.Microsecond
		}
		current := orientationSelectorV2Records(testCase.Shape.QualificationSplit, choice, shadow, forward, reverse, guarded, false)
		fixture, err := fixtureMetadata("unused", testCase.Dataset)
		require.NoError(t, err)
		fixture.PhysicalValidated = true
		fixture.PhysicalNodeCount, fixture.PhysicalEdgeCount = int64(fixture.NodeCount), int64(fixture.EdgeCount)
		fixture.NodeRelationBytes, fixture.EdgeRelationBytes = int64(fixture.NodeCount*1024), int64(fixture.EdgeCount*1024)
		for _, records := range [][]CaseResult{current.shadow, current.incumbent, current.reverse, current.guarded} {
			for index := range records {
				record := &records[index]
				record.Source, record.Dataset, record.Name, record.Category, record.Shape = testCase.Source, testCase.Dataset, testCase.Name, testCase.Category, testCase.Shape
				record.WorkloadSHA256 = scaleCaseWorkloadIdentity(testCase, ModePostgresSQL)
				attachFixtureMetadata(record, fixture)
				record.Environment.DirtyDiffSHA256 = cleanWorkingTreeSHA256()
				record.PostgresEnvironment.NodeRelationBytes = fixture.NodeRelationBytes
				record.PostgresEnvironment.EdgeRelationBytes = fixture.EdgeRelationBytes
				record.PostgresEnvironment.AnalyzeState = "edge:analyzed,node:analyzed"
			}
		}
		full = appendOrientationV2Artifacts(full, current)
	}
	training := orientationSelectorV2Artifacts{
		shadow:    cloneOrientationV2Split(full.shadow, "training"),
		incumbent: cloneOrientationV2Split(full.incumbent, "training"),
		reverse:   cloneOrientationV2Split(full.reverse, "training"),
		guarded:   cloneOrientationV2Split(full.guarded, "training"),
	}
	stampOrientationV2Selections(&training)
	stampOrientationV2Selections(&full)
	return training, full
}

// cloneOrientationV2Split returns an independent copy of orientation v2 split.
func cloneOrientationV2Split(records []CaseResult, split string) []CaseResult {
	result := make([]CaseResult, 0, len(records))
	for _, record := range records {
		if record.Shape.QualificationSplit != split {
			continue
		}
		copy := record
		if record.Environment != nil {
			environment := *record.Environment
			copy.Environment = &environment
		}
		result = append(result, copy)
	}
	return result
}

// compactOrientationV2Artifacts prepares or inspects test evidence for compact orientation v2 artifacts.
func compactOrientationV2Artifacts(artifacts orientationSelectorV2Artifacts, rounds, samples int) orientationSelectorV2Artifacts {
	compact := func(records []CaseResult) []CaseResult {
		result := make([]CaseResult, 0, len(records))
		for _, record := range records {
			if record.Environment.Round > rounds {
				continue
			}
			copy := record
			copy.Stats.Samples = append([]LatencySample(nil), record.Stats.Samples[:samples]...)
			result = append(result, copy)
		}
		return result
	}
	return orientationSelectorV2Artifacts{
		shadow:    compact(artifacts.shadow),
		incumbent: compact(artifacts.incumbent),
		reverse:   compact(artifacts.reverse),
		guarded:   compact(artifacts.guarded),
	}
}

// writeOrientationV2TestArtifact writes orientation v2 test artifact.
func writeOrientationV2TestArtifact(t *testing.T, path string, records []CaseResult) {
	t.Helper()
	output, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, writeJSONL(output, records))
	require.NoError(t, output.Close())
}

// orientationSelectorV2Records prepares or inspects test evidence for orientation selector v2 records.
func orientationSelectorV2Records(
	split, wouldSelect string,
	shadowDuration, incumbentDuration, reverseDuration, guardedDuration time.Duration,
	overflow bool,
) orientationSelectorV2Artifacts {
	const rounds = 12
	orders := [][4]int{
		{1, 2, 3, 4}, {2, 3, 4, 1}, {3, 4, 1, 2}, {4, 1, 2, 3},
		{1, 3, 4, 2}, {2, 4, 1, 3}, {3, 1, 2, 4}, {4, 2, 3, 1},
		{1, 4, 2, 3}, {2, 1, 3, 4}, {3, 2, 4, 1}, {4, 3, 1, 2},
	}
	artifacts := orientationSelectorV2Artifacts{}
	for round := 1; round <= rounds; round++ {
		order := orders[round-1]
		artifacts.shadow = append(artifacts.shadow, orientationSelectorV2Record(round, order[0], "shadow", split, wouldSelect, shadowDuration, overflow))
		artifacts.incumbent = append(artifacts.incumbent, orientationSelectorV2Record(round, order[1], "incumbent", split, "", incumbentDuration, false))
		artifacts.reverse = append(artifacts.reverse, orientationSelectorV2Record(round, order[2], "reverse", split, "", reverseDuration, false))
		artifacts.guarded = append(artifacts.guarded, orientationSelectorV2Record(round, order[3], "guarded", split, wouldSelect, guardedDuration, overflow))
	}
	stampOrientationV2Selections(&artifacts)
	return artifacts
}

// orientationSelectorV2Record prepares or inspects test evidence for orientation selector v2 record.
func orientationSelectorV2Record(round, armOrder int, arm, split, choice string, duration time.Duration, overflow bool) CaseResult {
	forward := string(optimize.ExpansionSearchStepwiseForward)
	reverse := string(optimize.ExpansionSearchSuffixSeededReverse)
	v2 := string(optimize.ExpansionSearchPolicyOrientationProbeV2)
	runtimeIdentity, emittedIdentity, selectorVersion := forward, forward, "fixed-suffix-static-v1"
	runtimeBranch, boundary, wouldSelect := "selected", optimize.ExpansionSearchExecutionBoundaryInlineStatement, ""
	fallback := false
	requested := forward
	if arm == "shadow" {
		emittedIdentity, selectorVersion, wouldSelect = v2, v2, choice
		runtimeBranch, requested = "shadow_incumbent", reverse
	}
	if arm == "reverse" {
		runtimeIdentity, emittedIdentity, selectorVersion, requested = reverse, reverse, "suffix-seeded-reverse-tool-v1", reverse
	}
	if arm == "guarded" {
		emittedIdentity, selectorVersion, boundary, requested = v2, v2, optimize.ExpansionSearchExecutionBoundaryGuardedDualArm, reverse
		if choice == reverse && !overflow {
			runtimeIdentity, runtimeBranch = reverse, "suffix_seeded_reverse"
		} else {
			runtimeIdentity, runtimeBranch = forward, "exact_forward_incumbent"
			fallback = overflow
		}
	}
	provenance := map[string]string{
		"requested_identity": "test", "planned_identities": "test", "emitted_identity": "test",
		"runtime_identity": "test", "applied_identity": "test", "selector_version": "test",
		"scheduler_version": "test", "runtime_branch": "test", "runtime_outcome_available": "test",
		"overflow": "test", "fallback_executed": "test", "execution_boundary": "test",
	}
	if wouldSelect != "" {
		provenance["would_select_identity"] = "test"
	}
	if fallback {
		provenance["fallback_identity"] = "test"
	}
	available := true
	record := CaseResult{
		Source:         "cases/orientation-v2.json",
		Dataset:        "orientation-v2-fixture",
		Name:           "fixed-suffix",
		Category:       "generated_fixed_suffix_expansion",
		WorkloadSHA256: sqlFingerprint("orientation-v2-workload"),
		ExecutionMode:  ModePostgresSQL,
		Status:         StatusOK,
		Shape: WorkloadShape{
			FixtureTier:        "normal",
			QualificationSplit: split,
		},
		RowCount:          1,
		ObservedRows:      []string{"[42]"},
		StableObservation: true,
		SQLFingerprint:    sqlFingerprint("orientation-v2-" + arm + "-sql"),
		Fixture: &FixtureMetadata{
			Dataset:           "orientation-v2-fixture",
			Checksum:          sqlFingerprint("orientation-v2-fixture-checksum"),
			NodeCount:         10,
			EdgeCount:         12,
			PhysicalValidated: true,
			PhysicalNodeCount: 10,
			PhysicalEdgeCount: 12,
			Configuration:     "orientation-v2-test",
		},
		PostgresEnvironment: &PostgresEnvironment{
			Version:              "PostgreSQL test",
			Database:             "dawgs",
			PlanCacheMode:        "auto",
			TransactionIsolation: "repeatable read",
			WorkMem:              "4MB",
			TempFileLimit:        "-1",
			GraphPartitionCount:  1,
			DatabaseOID:          1,
			Autovacuum:           "on",
			AnalyzeState:         "stable",
			SchemaFingerprint:    "schema",
			IndexFingerprint:     "index",
		},
		Environment: &RunEnvironment{
			ArtifactSchemaVersion: 2,
			CorpusSHA256:          testSHA("c"),
			SourceCommit:          "deadbeef",
			DirtyDiffSHA256:       testSHA("d"),
			BinarySHA256:          testSHA("b"),
			GOOS:                  "linux",
			GOARCH:                "amd64",
			CPUCount:              8,
			CPUModel:              "test-cpu",
			Kernel:                "test-kernel",
			CgroupCPU:             "max 100000",
			RunUUID:               fmt.Sprintf("orientation-v2-run-%d", round),
			Arm:                   arm,
			ArmOrder:              armOrder,
			Block:                 round,
			Round:                 round,
			WarmupIterations:      20,
			PoolSize:              1,
		},
		TraversalTelemetry: &TraversalExecutionTelemetry{
			SchemaVersion: TraversalExecutionTelemetrySchemaVersion,
			Level:         TraversalTelemetryLevelSummary,
			Summary: TraversalExecutionSummary{
				RequestedIdentity:       requested,
				PlannedIdentities:       []string{forward, reverse},
				EmittedIdentity:         emittedIdentity,
				RuntimeIdentity:         runtimeIdentity,
				AppliedIdentity:         runtimeIdentity,
				SelectorVersion:         selectorVersion,
				SchedulerVersion:        "not_applicable",
				ExecutionBoundary:       boundary,
				Caps:                    map[string]int64{},
				RuntimeOutcomeAvailable: &available,
				RuntimeBranch:           runtimeBranch,
				Overflow:                boolPointer(overflow),
				FallbackExecuted:        boolPointer(fallback),
				WouldSelectIdentity:     wouldSelect,
				Provenance:              provenance,
			},
		},
	}
	if fallback {
		record.TraversalTelemetry.Summary.FallbackIdentity = forward
	}
	record.Stats.WarmupIterations = 20
	for iteration := 1; iteration <= 50; iteration++ {
		sample := LatencySample{
			Round:             round,
			Block:             round,
			Arm:               arm,
			ArmOrder:          armOrder,
			RunUUID:           record.Environment.RunUUID,
			Iteration:         iteration,
			Classification:    "warm",
			Duration:          duration,
			RequestedIdentity: requested,
			RuntimeIdentity:   runtimeIdentity,
			RuntimeBranch:     runtimeBranch,
			FallbackExecuted:  boolPointer(fallback),
		}
		if arm == "shadow" || arm == "guarded" {
			sample.RuntimeAttestation = "timed_invocation"
			sample.RuntimeReceiptEvents = []RuntimeReceiptEvent{{
				Ordinal:          1,
				RuntimeIdentity:  runtimeIdentity,
				RuntimeBranch:    runtimeBranch,
				FallbackExecuted: fallback,
			}}
		} else {
			sample.RuntimeAttestation = "same_case_invocation_local_replay"
		}
		record.Stats.Samples = append(record.Stats.Samples, sample)
	}
	return record
}

// renameOrientationV2Records prepares or inspects test evidence for rename orientation v2 records.
func renameOrientationV2Records(name string, artifacts orientationSelectorV2Artifacts) {
	for _, records := range [][]CaseResult{artifacts.shadow, artifacts.incumbent, artifacts.reverse, artifacts.guarded} {
		for index := range records {
			records[index].Name = name
			records[index].Dataset = "generated_fixed_suffix_expansion_v3_" + name
			records[index].WorkloadSHA256 = sqlFingerprint("orientation-v2-workload-" + name)
			records[index].Fixture.Dataset = records[index].Dataset
			records[index].Fixture.Checksum = sqlFingerprint("orientation-v2-fixture-" + name)
			records[index].PostgresEnvironment.NodeRelationBytes = int64(len(name) * 1024)
			records[index].PostgresEnvironment.EdgeRelationBytes = int64(len(name) * 2048)
		}
	}
	stampOrientationV2Selections(&artifacts)
}

// appendOrientationV2Artifacts appends orientation v2 artifacts.
func appendOrientationV2Artifacts(values ...orientationSelectorV2Artifacts) orientationSelectorV2Artifacts {
	result := orientationSelectorV2Artifacts{}
	for _, value := range values {
		result.shadow = append(result.shadow, value.shadow...)
		result.incumbent = append(result.incumbent, value.incumbent...)
		result.reverse = append(result.reverse, value.reverse...)
		result.guarded = append(result.guarded, value.guarded...)
	}
	stampOrientationV2Selections(&result)
	return result
}

// stampOrientationV2Selections prepares or inspects test evidence for stamp orientation v2 selections.
func stampOrientationV2Selections(artifacts *orientationSelectorV2Artifacts) {
	if artifacts == nil {
		return
	}
	keys := map[performanceKey]struct{}{}
	for _, record := range artifacts.shadow {
		keys[performanceKey{
			dataset: record.Dataset,
			name:    record.Name,
			backend: record.ExecutionMode,
		}] = struct{}{}
	}
	declared := make([]DeclaredCaseBackend, 0, 2*len(keys))
	resolved := make([]ResolvedCaseSelector, 0, len(keys))
	for _, key := range sortedPerformanceKeys(keys) {
		for _, backend := range []ExecutionMode{ModePostgresSQL, ModeNeo4j} {
			declared = append(declared, DeclaredCaseBackend{
				Dataset: key.dataset,
				Name:    key.name,
				Backend: backend,
			})
		}
		resolved = append(resolved, ResolvedCaseSelector{
			Dataset:  key.dataset,
			Name:     key.name,
			Category: "generated_fixed_suffix_expansion",
		})
	}
	selection := &SelectionManifest{
		Version:                  selectionManifestVersion,
		Resolved:                 resolved,
		DiagnosticOnly:           true,
		FullDeclarationCount:     2 * len(keys),
		SelectedDeclarationCount: 2 * len(keys),
		DeclarationSHA256:        declarationSHA256(declared),
	}
	for _, records := range [][]CaseResult{artifacts.shadow, artifacts.incumbent, artifacts.reverse, artifacts.guarded} {
		for index := range records {
			copy := *selection
			copy.Resolved = append([]ResolvedCaseSelector(nil), selection.Resolved...)
			records[index].Environment.Selection = &copy
		}
	}
}

// boolPointer returns an addressable representation of bool.
func boolPointer(value bool) *bool { return &value }

// testSHA prepares or inspects test evidence for test sha.
func testSHA(digit string) string {
	value := ""
	for len(value) < 64 {
		value += digit
	}
	return value[:64]
}

// testOrientationV2Freeze prepares or inspects test evidence for test orientation v2 freeze.
func testOrientationV2Freeze() *OrientationSelectorV2FreezeManifest {
	return &OrientationSelectorV2FreezeManifest{
		Version: 1,
		Policy:  string(optimize.ExpansionSearchPolicyOrientationProbeV2),
		Formula: "F2=root_rows+maximum_depth*forward_degree_rows;R2=suffix_rows+boundary_rows+reverse_degree_rows;reverse=complete&&4*R2<3*F2",
		Caps: map[string]int64{
			"root_row_limit": optimize.ExpansionSearchOrientationRootRowLimit, "reverse_seed_row_limit": optimize.ExpansionSearchOrientationReverseSeedRowLimit,
			"directional_degree_row_limit": optimize.ExpansionSearchOrientationDirectionalDegreeRowLimit, "state_limit": optimize.ExpansionSearchOrientationStateLimit,
		},
		SourceCommit:          "deadbeef",
		DirtyDiffSHA256:       testSHA("d"),
		BinarySHA256:          testSHA("b"),
		DiscoveryReportSHA256: testSHA("e"),
	}
}
