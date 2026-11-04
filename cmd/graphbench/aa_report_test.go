// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
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
	require.NotNil(t, report.PhysicalChronology)
	require.True(t, report.PhysicalChronology.Validated)
	require.Equal(t, 5, report.PhysicalChronology.Rounds)
	require.Equal(t, 100, report.Cases[0].SamplesPerArm)
	require.InDelta(t, 1, report.Cases[0].P50.Ratio.Estimate, 0.0001)
	require.False(t, report.Cases[0].P99Gated)
	require.Contains(t, report.Cases[0].P99Reason, "diagnostic only")
}

// TestBuildAAResolutionReportRejectsPhysicalChronologyTampering verifies an
// alternating label schedule cannot hide fixed-order, overlapping, or detached
// process execution in the immutable A/A source artifacts.
func TestBuildAAResolutionReportRejectsPhysicalChronologyTampering(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func([]CaseResult)
		problem string
	}{
		{
			name: "fixed physical order behind alternating labels",
			mutate: func(records []CaseResult) {
				for index := range records {
					environment := records[index].Environment
					roundStarted := time.Unix(1_700_000_000+int64(environment.Round)*10, 0).UTC()
					if environment.Arm == "aa-b" {
						roundStarted = roundStarted.Add(2 * time.Second)
					}
					environment.StartedAt = roundStarted
					environment.EndedAt = roundStarted.Add(time.Second)
				}
			},
			problem: "arm timestamps contradict the declared execution order",
		},
		{
			name: "block differs from round",
			mutate: func(records []CaseResult) {
				for recordIndex := range records {
					if records[recordIndex].Environment.Round != 2 {
						continue
					}
					records[recordIndex].Environment.Block = 1
					for sampleIndex := range records[recordIndex].Stats.Samples {
						records[recordIndex].Stats.Samples[sampleIndex].Block = 1
					}
				}
			},
			problem: "requires block equal to round",
		},
		{
			name: "sample labels detached from process",
			mutate: func(records []CaseResult) {
				records[0].Environment.ArmOrder = 2
			},
			problem: "outside its physical arm invocation",
		},
		{
			name: "round overlaps prior round",
			mutate: func(records []CaseResult) {
				priorEnded := time.Time{}
				for index := range records {
					if records[index].Environment.Round == 1 && records[index].Environment.ArmOrder == 2 {
						priorEnded = records[index].Environment.EndedAt
					}
				}
				firstStarted := priorEnded.Add(-500 * time.Millisecond)
				for index := range records {
					environment := records[index].Environment
					if environment.Round != 2 {
						continue
					}
					environment.StartedAt = firstStarted.Add(time.Duration(environment.ArmOrder-1) * 2 * time.Second)
					environment.EndedAt = environment.StartedAt.Add(time.Second)
				}
			},
			problem: "overlaps or predates the prior round",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			records := explicitAARecords(t, 5, 20)
			test.mutate(records)
			_, err := buildAAResolutionReport(records, PerfGateOptions{
				Seed: 1, Confidence: 0.95, BootstrapCount: 10,
			})
			require.ErrorContains(t, err, test.problem)
		})
	}
}

// TestBuildAAResolutionReportRejectsSyntheticSingleStream verifies unlabeled samples cannot be relabeled after timing to manufacture A/A evidence.
func TestBuildAAResolutionReportRejectsSyntheticSingleStream(t *testing.T) {
	record := perfGateRecord("case", ModePostgresSQL, time.Millisecond, 5, 40)
	_, err := buildAAResolutionReport([]CaseResult{record}, PerfGateOptions{
		Seed:           1,
		Confidence:     0.95,
		BootstrapCount: 100,
	})
	require.ErrorContains(t, err, "without explicit round, block, arm, order, and run UUID")
}

// TestCreateAAResolutionReportAcceptsSeparateArmArtifacts verifies the native
// multi-input path combines two immutable append-series arms and binds both
// exact files into one report checksum.
func TestCreateAAResolutionReportAcceptsSeparateArmArtifacts(t *testing.T) {
	paths := []string{filepath.Join(t.TempDir(), "aa-a.jsonl"), filepath.Join(t.TempDir(), "aa-b.jsonl")}
	records := explicitAARecords(t, 5, 10)
	var left, right []CaseResult
	for _, record := range records {
		if record.Stats.Samples[0].Arm == "aa-a" {
			left = append(left, record)
		} else {
			right = append(right, record)
		}
	}
	require.NoError(t, writeJSONLFile(paths[0], left))
	require.NoError(t, writeJSONLFile(paths[1], right))

	output := filepath.Join(t.TempDir(), "aa.json")
	require.NoError(t, createAAResolutionReport(paths, output, PerfGateOptions{
		Seed:           1,
		Confidence:     0.95,
		BootstrapCount: 100,
	}))

	report, _, err := loadAAResolutionReport(output)
	require.NoError(t, err)
	require.Len(t, report.Cases, 1)
	require.True(t, validSHA256(report.ArtifactSHA256))
	require.NotNil(t, report.PhysicalChronology)
	require.Equal(t, report.ArtifactSHA256, report.PhysicalChronology.ArtifactSHA256)
	leftDigest, err := fileSHA256(paths[0])
	require.NoError(t, err)
	require.NotEqual(t, leftDigest, report.ArtifactSHA256)
}

// explicitAARecords prepares or inspects test evidence for explicit aa records.
func explicitAARecords(t *testing.T, rounds, samples int) []CaseResult {
	t.Helper()
	var records []CaseResult
	for round := 1; round <= rounds; round++ {
		for armIndex, arm := range []string{"aa-a", "aa-b"} {
			record := perfGateRecord("case", ModePostgresSQL, time.Millisecond, 1, samples)
			record.SQLFingerprint = "identical-sql"
			record.WorkloadSHA256 = "identical-workload"
			armOrder := 1 + (armIndex+round-1)%2
			roundStarted := time.Unix(1_700_000_000+int64(round)*10, 0).UTC()
			record.Environment.Round = round
			record.Environment.Block = round
			record.Environment.Arm = arm
			record.Environment.ArmOrder = armOrder
			record.Environment.RunUUID = "aa-run"
			record.Environment.StartedAt = roundStarted.Add(time.Duration(armOrder-1) * 2 * time.Second)
			record.Environment.EndedAt = record.Environment.StartedAt.Add(time.Second)
			for idx := range record.Stats.Samples {
				record.Stats.Samples[idx].Round = round
				record.Stats.Samples[idx].Block = round
				record.Stats.Samples[idx].Arm = arm
				record.Stats.Samples[idx].ArmOrder = armOrder
				record.Stats.Samples[idx].RunUUID = "aa-run"
			}
			records = append(records, record)
		}
	}
	return records
}
