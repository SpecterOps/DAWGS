// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/stretchr/testify/require"
)

func TestOrientationSelectorReportPassesMatchedLowRegretLowOverheadEvidence(t *testing.T) {
	trainingShadow, trainingIncumbent, trainingReverse := orientationSelectorRecords(
		"training",
		string(optimize.ExpansionSearchSuffixSeededReverse),
		10*time.Millisecond+50*time.Microsecond,
		10*time.Millisecond,
		5*time.Millisecond,
	)
	holdoutShadow, holdoutIncumbent, holdoutReverse := orientationSelectorRecords(
		"holdout",
		string(optimize.ExpansionSearchSuffixSeededReverse),
		10*time.Millisecond+50*time.Microsecond,
		10*time.Millisecond,
		5*time.Millisecond,
	)
	renameOrientationRecords("training-fixed-suffix", trainingShadow, trainingIncumbent, trainingReverse)
	renameOrientationRecords("holdout-fixed-suffix", holdoutShadow, holdoutIncumbent, holdoutReverse)
	shadow := append(trainingShadow, holdoutShadow...)
	incumbent := append(trainingIncumbent, holdoutIncumbent...)
	reverse := append(trainingReverse, holdoutReverse...)

	report, err := buildOrientationSelectorReport(shadow, incumbent, reverse, testAAReportForRecords(t, incumbent), OrientationSelectorReportOptions{
		Seed: 7, Confidence: defaultConfidenceLevel, BootstrapCount: 100, Protocol: referencePairProtocolConfirmation,
	})

	require.NoError(t, err)
	require.True(t, report.EvidencePassed)
	require.Equal(t, 1, report.TrainingCases)
	require.Equal(t, 1, report.HoldoutCases)
	require.True(t, report.TrainingPassed)
	require.True(t, report.HoldoutPassed)
	require.True(t, report.QualificationPassed)
	require.Equal(t, 1.10, report.SelectorRegretRatioLimit)
	require.Equal(t, 1.10, report.ProbeOverheadRatioLimit)
	require.Equal(t, 100*time.Microsecond, report.ProbeOverheadAbsoluteLimit)
	require.Len(t, report.Cases, 2)
	entry := report.Cases[1]
	if entry.QualificationSplit != "training" {
		entry = report.Cases[0]
	}
	require.Equal(t, "training", entry.QualificationSplit)
	require.Equal(t, "selector_training", entry.QualificationRole)
	require.True(t, entry.ThresholdTuningEligible)
	require.True(t, entry.QualificationEligible)
	require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), entry.WouldSelectIdentity)
	require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), entry.FastestExactIdentity)
	require.True(t, entry.SelectorRegret.Passed)
	require.True(t, entry.ProbeOverhead.Passed)
	require.True(t, entry.ExactObservationsMatched)
}

func TestOrientationSelectorReportFailsRegretWhenShadowChoosesSlowArm(t *testing.T) {
	shadow, incumbent, reverse := orientationSelectorRecords(
		"training",
		string(optimize.ExpansionSearchStepwiseForward),
		10*time.Millisecond+25*time.Microsecond,
		10*time.Millisecond,
		time.Millisecond,
	)

	report, err := buildOrientationSelectorReport(shadow, incumbent, reverse, testAAReportForRecords(t, incumbent), OrientationSelectorReportOptions{
		Seed: 11, Confidence: defaultConfidenceLevel, BootstrapCount: 100, Protocol: referencePairProtocolConfirmation,
	})

	require.NoError(t, err)
	require.False(t, report.EvidencePassed)
	require.False(t, report.TrainingPassed)
	require.False(t, report.HoldoutPassed)
	require.False(t, report.QualificationPassed)
	require.False(t, report.Cases[0].SelectorRegret.Passed)
	require.True(t, report.Cases[0].ProbeOverhead.Passed)
	require.Contains(t, report.Cases[0].Reasons, "selector regret exceeds the 1.10/A/A floor")
}

func TestOrientationSelectorReportAllowsAbsoluteProbeFloor(t *testing.T) {
	shadow, incumbent, reverse := orientationSelectorRecords(
		"training",
		string(optimize.ExpansionSearchStepwiseForward),
		250*time.Microsecond,
		200*time.Microsecond,
		300*time.Microsecond,
	)

	report, err := buildOrientationSelectorReport(shadow, incumbent, reverse, testAAReportForRecords(t, incumbent), OrientationSelectorReportOptions{
		Seed: 13, Confidence: defaultConfidenceLevel, BootstrapCount: 100, Protocol: referencePairProtocolConfirmation,
	})

	require.NoError(t, err)
	probe := report.Cases[0].ProbeOverhead
	require.Greater(t, probe.Ratio.Upper, 1.10)
	require.Equal(t, 50*time.Microsecond, probe.AbsoluteGapUpper)
	require.True(t, probe.Passed)
}

func TestOrientationSelectorReportKeepsHoldoutEvaluationOnlyAndExcludesDiagnostic(t *testing.T) {
	for _, testCase := range []struct {
		split                 string
		role                  string
		qualificationEligible bool
		qualificationPassed   bool
	}{
		{split: "holdout", role: "frozen_evaluation", qualificationEligible: true, qualificationPassed: false},
		{split: "diagnostic", role: "diagnostic_only", qualificationEligible: false, qualificationPassed: false},
	} {
		t.Run(testCase.split, func(t *testing.T) {
			shadow, incumbent, reverse := orientationSelectorRecords(
				testCase.split,
				string(optimize.ExpansionSearchSuffixSeededReverse),
				10*time.Millisecond,
				10*time.Millisecond,
				5*time.Millisecond,
			)
			report, err := buildOrientationSelectorReport(shadow, incumbent, reverse, testAAReportForRecords(t, incumbent), OrientationSelectorReportOptions{
				Seed: 17, Confidence: defaultConfidenceLevel, BootstrapCount: 50, Protocol: referencePairProtocolConfirmation,
			})
			require.NoError(t, err)
			require.Equal(t, testCase.role, report.Cases[0].QualificationRole)
			require.False(t, report.Cases[0].ThresholdTuningEligible)
			require.Equal(t, testCase.qualificationEligible, report.Cases[0].QualificationEligible)
			require.Equal(t, testCase.qualificationPassed, report.QualificationPassed)
		})
	}
}

func TestOrientationSelectorReportRequiresPassingTrainingAndFrozenHoldout(t *testing.T) {
	trainingShadow, trainingIncumbent, trainingReverse := orientationSelectorRecords(
		"training", string(optimize.ExpansionSearchSuffixSeededReverse), 10*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond,
	)
	holdoutShadow, holdoutIncumbent, holdoutReverse := orientationSelectorRecords(
		"holdout", string(optimize.ExpansionSearchStepwiseForward), 10*time.Millisecond, 10*time.Millisecond, time.Millisecond,
	)
	renameOrientationRecords("training-pass", trainingShadow, trainingIncumbent, trainingReverse)
	renameOrientationRecords("holdout-fail", holdoutShadow, holdoutIncumbent, holdoutReverse)
	shadow := append(trainingShadow, holdoutShadow...)
	incumbent := append(trainingIncumbent, holdoutIncumbent...)
	reverse := append(trainingReverse, holdoutReverse...)

	report, err := buildOrientationSelectorReport(shadow, incumbent, reverse, testAAReportForRecords(t, incumbent), OrientationSelectorReportOptions{
		Seed: 19, Confidence: defaultConfidenceLevel, BootstrapCount: 100, Protocol: referencePairProtocolConfirmation,
	})
	require.NoError(t, err)
	require.True(t, report.TrainingPassed)
	require.False(t, report.HoldoutPassed)
	require.False(t, report.QualificationPassed)
}

func TestOrientationSelectorReportRejectsSplitDriftAndNonIncumbentShadowRuntime(t *testing.T) {
	shadow, incumbent, reverse := orientationSelectorRecords(
		"training",
		string(optimize.ExpansionSearchSuffixSeededReverse),
		10*time.Millisecond,
		10*time.Millisecond,
		5*time.Millisecond,
	)
	reverse[0].Shape.QualificationSplit = "holdout"
	_, err := buildOrientationSelectorReport(shadow, incumbent, reverse, testAAReportForRecords(t, incumbent), OrientationSelectorReportOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 10, Protocol: referencePairProtocolConfirmation,
	})
	require.ErrorContains(t, err, "changes qualification split")

	reverse[0].Shape.QualificationSplit = "training"
	shadow[0].TraversalTelemetry.Summary.RuntimeIdentity = string(optimize.ExpansionSearchSuffixSeededReverse)
	_, err = buildOrientationSelectorReport(shadow, incumbent, reverse, testAAReportForRecords(t, incumbent), OrientationSelectorReportOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 10, Protocol: referencePairProtocolConfirmation,
	})
	require.ErrorContains(t, err, "incumbent-only")
}

func TestOrientationSelectorReportRejectsUnbalancedThreeArmOrder(t *testing.T) {
	shadow, incumbent, reverse := orientationSelectorRecords(
		"training",
		string(optimize.ExpansionSearchSuffixSeededReverse),
		10*time.Millisecond,
		10*time.Millisecond,
		5*time.Millisecond,
	)
	for recordIndex := range reverse {
		for sampleIndex := range reverse[recordIndex].Stats.Samples {
			reverse[recordIndex].Stats.Samples[sampleIndex].ArmOrder = 3
		}
	}
	_, err := buildOrientationSelectorReport(shadow, incumbent, reverse, testAAReportForRecords(t, incumbent), OrientationSelectorReportOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 10, Protocol: referencePairProtocolConfirmation,
	})
	require.ErrorContains(t, err, "duplicate three-arm order")
}

func orientationSelectorRecords(
	split, wouldSelect string,
	shadowDuration, incumbentDuration, reverseDuration time.Duration,
) (shadow, incumbent, reverse []CaseResult) {
	const rounds = 12
	orders := [][3]int{
		{1, 2, 3},
		{2, 3, 1},
		{3, 1, 2},
		{1, 3, 2},
		{2, 1, 3},
		{3, 2, 1},
	}
	for round := 1; round <= rounds; round++ {
		order := orders[(round-1)%len(orders)]
		shadow = append(shadow, orientationSelectorRecord(round, order[0], "shadow", split, wouldSelect, shadowDuration))
		incumbent = append(incumbent, orientationSelectorRecord(round, order[1], "incumbent", split, "", incumbentDuration))
		reverse = append(reverse, orientationSelectorRecord(round, order[2], "reverse", split, "", reverseDuration))
	}
	return shadow, incumbent, reverse
}

func orientationSelectorRecord(round, armOrder int, arm, split, wouldSelect string, duration time.Duration) CaseResult {
	forward := string(optimize.ExpansionSearchStepwiseForward)
	reverse := string(optimize.ExpansionSearchSuffixSeededReverse)
	runtimeIdentity := forward
	emittedIdentity := forward
	selectorVersion := "static-lowering-v1"
	runtimeBranch := "selected"
	if arm == "shadow" {
		emittedIdentity = string(optimize.ExpansionSearchPolicyOrientationProbeV1)
		selectorVersion = emittedIdentity
		runtimeBranch = "shadow_incumbent"
	}
	if arm == "reverse" {
		runtimeIdentity = reverse
		emittedIdentity = reverse
		selectorVersion = "suffix-seeded-reverse-tool-v1"
	}
	fallback := false
	overflow := false
	record := CaseResult{
		Dataset:             "orientation-fixture",
		Name:                "fixed-suffix",
		Category:            "generated_fixed_suffix_expansion_v2",
		WorkloadSHA256:      "orientation-workload-v1",
		ExecutionMode:       ModePostgresSQL,
		Status:              StatusOK,
		Shape:               WorkloadShape{FixtureTier: "normal", QualificationSplit: split},
		RowCount:            1,
		ObservedRows:        []string{"[42]"},
		StableObservation:   true,
		SQLFingerprint:      "orientation-" + arm + "-sql-v1",
		PostgresEnvironment: &PostgresEnvironment{PlanCacheMode: "auto"},
		Environment: &RunEnvironment{
			Arm: arm, ArmOrder: armOrder, Block: round, Round: round,
			RunUUID: "orientation-run-" + fmt.Sprint(round), BinarySHA256: "orientation-binary-v1",
			GOOS: "linux", GOARCH: "amd64", CPUCount: 8, CPUModel: "test-cpu", Kernel: "test-kernel", CgroupCPU: "max 100000",
			WarmupIterations: 20,
		},
		TraversalTelemetry: &TraversalExecutionTelemetry{
			SchemaVersion: TraversalExecutionTelemetrySchemaVersion,
			Level:         TraversalTelemetryLevelSummary,
			Summary: TraversalExecutionSummary{
				RequestedIdentity:   reverse,
				PlannedIdentities:   []string{forward, reverse},
				EmittedIdentity:     emittedIdentity,
				RuntimeIdentity:     runtimeIdentity,
				AppliedIdentity:     runtimeIdentity,
				SelectorVersion:     selectorVersion,
				SchedulerVersion:    "not_applicable",
				Caps:                map[string]int64{},
				RuntimeBranch:       runtimeBranch,
				Overflow:            &overflow,
				FallbackExecuted:    &fallback,
				WouldSelectIdentity: wouldSelect,
				Provenance:          map[string]string{},
			},
		},
	}
	record.Stats.WarmupIterations = 20
	for iteration := 1; iteration <= 50; iteration++ {
		record.Stats.Samples = append(record.Stats.Samples, LatencySample{
			Round: round, Block: round, Arm: arm, ArmOrder: armOrder,
			RunUUID: record.Environment.RunUUID, Iteration: iteration,
			Classification: "warm", Duration: duration,
		})
	}
	return record
}

func renameOrientationRecords(name string, artifacts ...[]CaseResult) {
	for _, records := range artifacts {
		for index := range records {
			records[index].Name = name
			records[index].WorkloadSHA256 = "orientation-workload-" + name
		}
	}
}
