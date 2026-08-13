// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBuildConfirmationReportClassifiesFreshMatchedP95 verifies that distinct predecessor and candidate binaries with a measurable P95 increase produce a comparable causal confirmation.
func TestBuildConfirmationReportClassifiesFreshMatchedP95(t *testing.T) {
	left := []CaseResult{confirmationRecord("alert", "predecessor", "binary-a", 10*time.Millisecond)}
	right := []CaseResult{confirmationRecord("alert", "candidate", "binary-b", 13*time.Millisecond)}
	stampPairedEvidence(left, right, 20)

	report, err := buildConfirmationReport(left, right, testAAReportForRecords(t, left), ConfirmationOptions{
		Seed:           7,
		Confidence:     0.95,
		BootstrapCount: 100,
		CaseNames:      []string{"alert"},
	})

	require.NoError(t, err)
	require.Equal(t, "causal_confirmation", report.Kind)
	require.Equal(t, "confirmed", report.Cases[0].P95.Classification)
	require.Equal(t, 3*time.Millisecond, report.Cases[0].P95.AbsoluteChange.Estimate)
	require.True(t, report.Cases[0].Comparable)
	require.False(t, report.PromotionEligible)
}

// TestBuildConfirmationReportRecognizesSameBinaryBlockAA verifies that identical binaries are classified as a reload control and clear a non-inferior result.
func TestBuildConfirmationReportRecognizesSameBinaryBlockAA(t *testing.T) {
	left := []CaseResult{confirmationRecord("control", "block-a", "same", 10*time.Millisecond)}
	right := []CaseResult{confirmationRecord("control", "block-b", "same", 10*time.Millisecond)}
	stampPairedEvidence(left, right, 20)

	report, err := buildConfirmationReport(left, right, nil, ConfirmationOptions{
		Seed:           1,
		Confidence:     0.95,
		BootstrapCount: 50,
	})
	require.NoError(t, err)
	require.Equal(t, "block_reload_aa", report.Kind)
	require.Equal(t, "cleared_non_inferior", report.Cases[0].Disposition)
}

// TestBuildConfirmationReportAllowsIntentionalCrossArmSQLAndPlanChanges verifies that implementation changes between predecessor and candidate arms do not invalidate an otherwise controlled comparison.
func TestBuildConfirmationReportAllowsIntentionalCrossArmSQLAndPlanChanges(t *testing.T) {
	left := []CaseResult{confirmationRecord("changed", "predecessor", "binary-a", 10*time.Millisecond)}
	right := []CaseResult{confirmationRecord("changed", "candidate", "binary-b", 5*time.Millisecond)}
	left[0].SQLFingerprint = "incumbent-sql"
	right[0].SQLFingerprint = "candidate-sql"
	left[0].PostgresPlan = []string{"CTE Scan on incumbent"}
	right[0].PostgresPlan = []string{"Recursive Union"}
	stampPairedEvidence(left, right, 20)

	report, err := buildConfirmationReport(left, right, testAAReportForRecords(t, left), ConfirmationOptions{
		Seed:           1,
		Confidence:     0.95,
		BootstrapCount: 50,
		CaseNames:      []string{"changed"},
	})
	require.NoError(t, err)
	require.True(t, report.Cases[0].Comparable)
	require.True(t, report.PromotionEligible)
}

// TestConfirmationComparableRejectsFingerprintChangeWithinArm verifies that SQL drift among repetitions of one arm makes the confirmation comparison invalid.
func TestConfirmationComparableRejectsFingerprintChangeWithinArm(t *testing.T) {
	left := []CaseResult{
		confirmationRecord("changed", "predecessor", "binary-a", 10*time.Millisecond),
		confirmationRecord("changed", "predecessor", "binary-a", 10*time.Millisecond),
	}
	right := []CaseResult{confirmationRecord("changed", "candidate", "binary-b", 5*time.Millisecond)}
	left[1].SQLFingerprint = "unstable-sql"

	comparable, reasons := confirmationComparable(left, right, performanceKey{
		dataset: left[0].Dataset,
		name:    "changed",
		backend: ModePostgresSQL,
	})
	require.False(t, comparable)
	require.Contains(t, reasons, "SQL fingerprint changes within arm")
}

// TestPostgresPlanShapeIgnoresReloadedEntityIDs verifies that literal database IDs and timing noise do not alter the normalized PostgreSQL plan fingerprint.
func TestPostgresPlanShapeIgnoresReloadedEntityIDs(t *testing.T) {
	left := []string{"Index Cond: (id = '4624444'::bigint)", "Planning Time: 0.408 ms", "Execution Time: 0.224 ms"}
	right := []string{"Index Cond: (id = '4630087'::bigint)", "Planning Time: 0.189 ms", "Execution Time: 0.093 ms"}
	require.Equal(t, postgresPlanShapeSHA256(left), postgresPlanShapeSHA256(right))
}

// TestBuildConfirmationReportRejectsUnknownExactCase verifies that an exact selector must resolve to an observed case instead of yielding an empty confirmation report.
func TestBuildConfirmationReportRejectsUnknownExactCase(t *testing.T) {
	record := confirmationRecord("present", "arm", "binary", time.Millisecond)
	_, err := buildConfirmationReport([]CaseResult{record}, []CaseResult{record}, nil, ConfirmationOptions{
		Seed:           1,
		Confidence:     0.95,
		BootstrapCount: 10,
		CaseNames:      []string{"missing"},
	})
	require.ErrorContains(t, err, "unknown confirmation case")
}

// TestBuildConfirmationReportRequiresHostAAForCausalPromotion verifies a fresh binary comparison fails closed without per-case host calibration.
func TestBuildConfirmationReportRequiresHostAAForCausalPromotion(t *testing.T) {
	left := []CaseResult{confirmationRecord("changed", "predecessor", "binary-a", time.Millisecond)}
	right := []CaseResult{confirmationRecord("changed", "candidate", "binary-b", 900*time.Microsecond)}
	stampPairedEvidence(left, right, 20)

	_, err := buildConfirmationReport(left, right, nil, ConfirmationOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 10, CaseNames: []string{"changed"},
	})

	require.ErrorContains(t, err, "host A/A resolution report is required")
}

func TestSameExecutableRequiresSameEffectiveTreatment(t *testing.T) {
	left := []CaseResult{confirmationRecord("changed", "a1", "shared-binary", time.Millisecond)}
	right := []CaseResult{confirmationRecord("changed", "i1", "shared-binary", time.Millisecond)}
	left[0].SQLFingerprint = "a1-sql"
	right[0].SQLFingerprint = "i1-sql"
	require.False(t, sameExecutable(left, right))

	right[0].SQLFingerprint = left[0].SQLFingerprint
	right[0].Environment.Invocation = append(right[0].Environment.Invocation, "--postgres-force-shortest-executor=ASP-I1-U-DAG+MAT-M0")
	require.False(t, sameExecutable(left, right))

	right[0].Environment.Invocation = append([]string(nil), left[0].Environment.Invocation...)
	require.True(t, sameExecutable(left, right))
}

// TestBuildConfirmationReportKeepsStressTimingDiagnostic verifies stress comparisons remain descriptive and need no promotion calibration.
func TestBuildConfirmationReportKeepsStressTimingDiagnostic(t *testing.T) {
	left := []CaseResult{confirmationRecord("stress", "predecessor", "binary-a", time.Millisecond)}
	right := []CaseResult{confirmationRecord("stress", "candidate", "binary-b", 10*time.Millisecond)}
	left[0].Shape.FixtureTier = "stress"
	right[0].Shape.FixtureTier = "stress"

	report, err := buildConfirmationReport(left, right, nil, ConfirmationOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 10, CaseNames: []string{"stress"},
	})

	require.NoError(t, err)
	require.False(t, report.PromotionEligible)
	require.False(t, report.Cases[0].TimingGated)
	require.Equal(t, "stress_diagnostic", report.Cases[0].Disposition)
}

// TestBuildConfirmationReportKeepsDiagnosticSplitOutOfPromotion verifies a
// normal-tier boundary case remains evaluation-only by declaration.
func TestBuildConfirmationReportKeepsDiagnosticSplitOutOfPromotion(t *testing.T) {
	left := []CaseResult{confirmationRecord("boundary", "predecessor", "binary-a", time.Millisecond)}
	right := []CaseResult{confirmationRecord("boundary", "candidate", "binary-b", 10*time.Millisecond)}
	left[0].Shape.QualificationSplit = "diagnostic"
	right[0].Shape.QualificationSplit = "diagnostic"

	report, err := buildConfirmationReport(left, right, nil, ConfirmationOptions{
		Confidence: defaultConfidenceLevel, BootstrapCount: 10, CaseNames: []string{"boundary"},
	})

	require.NoError(t, err)
	require.False(t, report.PromotionEligible)
	require.False(t, report.Cases[0].TimingGated)
	require.Equal(t, "qualification_diagnostic", report.Cases[0].Disposition)
}

// TestBuildConfirmationReportRequiresIndependentTraversalHoldout verifies a
// clean training result cannot qualify a traversal candidate without an
// independently named frozen-holdout case.
func TestBuildConfirmationReportRequiresIndependentTraversalHoldout(t *testing.T) {
	left := []CaseResult{
		confirmationRecord("sp-training", "predecessor", "binary-a", 10*time.Millisecond),
		confirmationRecord("sp-holdout", "predecessor", "binary-a", 10*time.Millisecond),
	}
	right := []CaseResult{
		confirmationRecord("sp-training", "candidate", "binary-b", 5*time.Millisecond),
		confirmationRecord("sp-holdout", "candidate", "binary-b", 5*time.Millisecond),
	}
	for _, records := range [][]CaseResult{left, right} {
		records[0].Category = "generated_shortest_path_v2"
		records[0].Shape.QualificationSplit = "training"
		records[1].Category = "generated_shortest_path_v2"
		records[1].Shape.QualificationSplit = "holdout"
	}
	stampPairedEvidence(left, right, 20)

	report, err := buildConfirmationReport(left, right, testAAReportForRecords(t, left), ConfirmationOptions{
		Seed: 1, Confidence: defaultConfidenceLevel, BootstrapCount: 50, CaseNames: []string{"sp-training", "sp-holdout"},
	})
	require.NoError(t, err)
	require.True(t, report.QualificationRequired)
	require.True(t, report.TrainingPassed)
	require.True(t, report.HoldoutPassed)
	require.True(t, report.QualificationPassed)
	require.True(t, report.PromotionEligible)

	left = left[:1]
	right = right[:1]
	report, err = buildConfirmationReport(left, right, testAAReportForRecords(t, left), ConfirmationOptions{
		Seed: 1, Confidence: defaultConfidenceLevel, BootstrapCount: 50, CaseNames: []string{"sp-training"},
	})
	require.NoError(t, err)
	require.True(t, report.TrainingPassed)
	require.False(t, report.HoldoutPassed)
	require.False(t, report.QualificationPassed)
	require.False(t, report.PromotionEligible)
}

// confirmationRecord returns a stable PostgreSQL observation annotated with the requested arm and binary identity.
func confirmationRecord(name, arm, binary string, duration time.Duration) CaseResult {
	record := perfGateRecord(name, ModePostgresSQL, duration, 10, 50)
	record.SQLFingerprint = "sql"
	record.ObservedRows = []string{"[1]"}
	record.Fixture = &FixtureMetadata{Checksum: "fixture"}
	record.Environment = &RunEnvironment{
		Arm:          arm,
		BinarySHA256: binary,
		GOOS:         "linux",
		GOARCH:       "amd64",
		CPUCount:     8,
		CPUModel:     "test-cpu",
		Kernel:       "test-kernel",
		CgroupCPU:    "max 100000",
	}
	return record
}
