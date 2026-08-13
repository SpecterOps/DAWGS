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

// TestBuildAAResolutionReportUsesExplicitMatchedArmsAndKeepsP99Diagnostic verifies that independently executed balanced arms produce a promotion-grade noise floor while P99 remains explicitly non-gating.
func TestBuildAAResolutionReportUsesExplicitMatchedArmsAndKeepsP99Diagnostic(t *testing.T) {
	records := explicitAARecords(t, 5, 20)
	report, err := buildAAResolutionReport(records, PerfGateOptions{
		Seed:           1,
		Confidence:     0.95,
		BootstrapCount: 100,
	})

	require.NoError(t, err)
	require.Len(t, report.Cases, 1)
	require.Equal(t, aaReportVersion, report.Version)
	require.True(t, validSHA256(report.HostFingerprint))
	require.True(t, report.OrderBalanced)
	require.Equal(t, 100, report.Cases[0].SamplesPerArm)
	require.InDelta(t, 1, report.Cases[0].P50.Ratio.Estimate, 0.0001)
	require.False(t, report.Cases[0].P99Gated)
	require.Contains(t, report.Cases[0].P99Reason, "diagnostic only")
}

// TestBuildAAResolutionReportRejectsSyntheticSingleStream verifies unlabeled samples cannot be relabeled after timing to manufacture A/A evidence.
func TestBuildAAResolutionReportRejectsSyntheticSingleStream(t *testing.T) {
	record := perfGateRecord("case", ModePostgresSQL, time.Millisecond, 5, 40)
	_, err := buildAAResolutionReport([]CaseResult{record}, PerfGateOptions{Seed: 1, Confidence: 0.95, BootstrapCount: 100})
	require.ErrorContains(t, err, "without explicit round, block, arm, order, and run UUID")
}

func explicitAARecords(t *testing.T, rounds, samples int) []CaseResult {
	t.Helper()
	var records []CaseResult
	for round := 1; round <= rounds; round++ {
		for armIndex, arm := range []string{"aa-a", "aa-b"} {
			record := perfGateRecord("case", ModePostgresSQL, time.Millisecond, 1, samples)
			record.SQLFingerprint = "identical-sql"
			record.WorkloadSHA256 = "identical-workload"
			for idx := range record.Stats.Samples {
				record.Stats.Samples[idx].Round = round
				record.Stats.Samples[idx].Block = round
				record.Stats.Samples[idx].Arm = arm
				record.Stats.Samples[idx].ArmOrder = 1 + (armIndex+round-1)%2
				record.Stats.Samples[idx].RunUUID = "aa-run"
			}
			records = append(records, record)
		}
	}
	return records
}
