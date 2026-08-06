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

func TestBuildConfirmationReportClassifiesFreshMatchedP95(t *testing.T) {
	left := []CaseResult{confirmationRecord("alert", "predecessor", "binary-a", 10*time.Millisecond)}
	right := []CaseResult{confirmationRecord("alert", "candidate", "binary-b", 13*time.Millisecond)}

	report, err := buildConfirmationReport(left, right, nil, ConfirmationOptions{
		Seed: 7, Confidence: 0.95, BootstrapCount: 100, CaseNames: []string{"alert"},
	})

	require.NoError(t, err)
	require.Equal(t, "causal_confirmation", report.Kind)
	require.Equal(t, "confirmed", report.Cases[0].P95.Classification)
	require.Equal(t, 3*time.Millisecond, report.Cases[0].P95.AbsoluteChange.Estimate)
	require.True(t, report.Cases[0].Comparable)
}

func TestBuildConfirmationReportRecognizesSameBinaryBlockAA(t *testing.T) {
	left := []CaseResult{confirmationRecord("control", "block-a", "same", 10*time.Millisecond)}
	right := []CaseResult{confirmationRecord("control", "block-b", "same", 10*time.Millisecond)}

	report, err := buildConfirmationReport(left, right, nil, ConfirmationOptions{Seed: 1, Confidence: 0.95, BootstrapCount: 50})
	require.NoError(t, err)
	require.Equal(t, "block_reload_aa", report.Kind)
	require.Equal(t, "cleared_non_inferior", report.Cases[0].Disposition)
}

func TestBuildConfirmationReportRejectsUnknownExactCase(t *testing.T) {
	record := confirmationRecord("present", "arm", "binary", time.Millisecond)
	_, err := buildConfirmationReport([]CaseResult{record}, []CaseResult{record}, nil, ConfirmationOptions{
		Seed: 1, Confidence: 0.95, BootstrapCount: 10, CaseNames: []string{"missing"},
	})
	require.ErrorContains(t, err, "unknown confirmation case")
}

func confirmationRecord(name, arm, binary string, duration time.Duration) CaseResult {
	record := perfGateRecord(name, ModePostgresSQL, duration, 10, 50)
	record.SQLFingerprint = "sql"
	record.ObservedRows = []string{"[1]"}
	record.Fixture = &FixtureMetadata{Checksum: "fixture"}
	record.Environment = &RunEnvironment{Arm: arm, BinarySHA256: binary}
	return record
}
