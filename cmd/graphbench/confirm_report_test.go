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

	report, err := buildConfirmationReport(left, right, nil, ConfirmationOptions{
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
}

// TestBuildConfirmationReportRecognizesSameBinaryBlockAA verifies that identical binaries are classified as a reload control and clear a non-inferior result.
func TestBuildConfirmationReportRecognizesSameBinaryBlockAA(t *testing.T) {
	left := []CaseResult{confirmationRecord("control", "block-a", "same", 10*time.Millisecond)}
	right := []CaseResult{confirmationRecord("control", "block-b", "same", 10*time.Millisecond)}

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

	report, err := buildConfirmationReport(left, right, nil, ConfirmationOptions{
		Seed:           1,
		Confidence:     0.95,
		BootstrapCount: 50,
		CaseNames:      []string{"changed"},
	})
	require.NoError(t, err)
	require.True(t, report.Cases[0].Comparable)
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

// confirmationRecord returns a stable PostgreSQL observation annotated with the requested arm and binary identity.
func confirmationRecord(name, arm, binary string, duration time.Duration) CaseResult {
	record := perfGateRecord(name, ModePostgresSQL, duration, 10, 50)
	record.SQLFingerprint = "sql"
	record.ObservedRows = []string{"[1]"}
	record.Fixture = &FixtureMetadata{Checksum: "fixture"}
	record.Environment = &RunEnvironment{
		Arm:          arm,
		BinarySHA256: binary,
	}
	return record
}
