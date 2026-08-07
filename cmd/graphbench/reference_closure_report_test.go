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

func TestBuildReferenceClosureReportPassesRatioOrResolution(t *testing.T) {
	records := referenceClosureRecords(10, 50, time.Millisecond, 1050*time.Microsecond)
	report, err := buildReferenceClosureReport(records, ReferenceClosureOptions{
		Seed: 7, Confidence: 0.975, BootstrapCount: 250,
	})

	require.NoError(t, err)
	require.True(t, report.Passed)
	require.Len(t, report.Cases, 1)
	entry := report.Cases[0]
	require.Equal(t, 10, entry.Rounds)
	require.Equal(t, 500, entry.ProductionSamples)
	require.Equal(t, 500, entry.ReferenceSamples)
	require.InDelta(t, 1.05, entry.MedianRatio.Estimate, 0.0001)
	require.LessOrEqual(t, entry.AbsoluteGapUpper, 100*time.Microsecond)
	require.Equal(t, 100*time.Microsecond, entry.AbsoluteFloor)
	require.Equal(t, 100*time.Microsecond, entry.AbsoluteResolution)
}

func TestBuildReferenceClosureReportUsesCaseAAResolution(t *testing.T) {
	records := referenceClosureRecords(10, 50, 2*time.Millisecond, 1500*time.Microsecond)
	for idx := range records {
		for sampleIdx := range records[idx].RawPGXWaterfall.Samples {
			if sampleIdx%2 == 1 {
				records[idx].RawPGXWaterfall.Samples[sampleIdx].Total = 2700 * time.Microsecond
			}
		}
	}
	report, err := buildReferenceClosureReport(records, ReferenceClosureOptions{
		Seed: 1, Confidence: 0.975, BootstrapCount: 100,
	})

	require.NoError(t, err)
	require.True(t, report.Passed)
	require.Greater(t, report.Cases[0].ProductionAAResolution, 100*time.Microsecond)
	require.Equal(t, report.Cases[0].ProductionAAResolution, report.Cases[0].AbsoluteResolution)
}

func TestBuildReferenceClosureReportFailsMaterialGap(t *testing.T) {
	records := referenceClosureRecords(10, 50, time.Millisecond, 1500*time.Microsecond)
	report, err := buildReferenceClosureReport(records, ReferenceClosureOptions{
		Seed: 1, Confidence: 0.975, BootstrapCount: 100,
	})

	require.NoError(t, err)
	require.False(t, report.Passed)
	require.ErrorContains(t, reasonsError(report.Cases[0].Reasons), "ratio upper")
}

func TestBuildReferenceClosureReportEnforcesProtocolAndExactComparator(t *testing.T) {
	records := referenceClosureRecords(9, 49, time.Millisecond, time.Millisecond)
	report, err := buildReferenceClosureReport(records, ReferenceClosureOptions{
		Seed: 1, Confidence: 0.975, BootstrapCount: 100,
	})
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.ErrorContains(t, reasonsError(report.Cases[0].Reasons), "10-20 matched rounds")
	require.ErrorContains(t, reasonsError(report.Cases[0].Reasons), "at least 50 samples")

	records = referenceClosureRecords(10, 50, time.Millisecond, time.Millisecond)
	records[0].PostgresReferences[0].ObservedRows = []string{"[2]"}
	_, err = buildReferenceClosureReport(records, ReferenceClosureOptions{Seed: 1, Confidence: 0.975})
	require.ErrorContains(t, err, "observation differs")

	records = referenceClosureRecords(10, 50, time.Millisecond, time.Millisecond)
	records[1].PostgresReferences[0].MeasurementOrder = 2
	_, err = buildReferenceClosureReport(records, ReferenceClosureOptions{Seed: 1, Confidence: 0.975})
	require.ErrorContains(t, err, "lacks carryover-balanced")
}

func referenceClosureRecords(rounds, samples int, referenceDuration, productionDuration time.Duration) []CaseResult {
	records := make([]CaseResult, 0, rounds)
	for round := 1; round <= rounds; round++ {
		productionOrder, referenceOrder := referenceClosureMeasurementOrder(true, round)
		record := CaseResult{
			Dataset: "fixture", Name: "distance", ExecutionMode: ModePostgresSQL, Status: StatusOK,
			RowCount: 1, ObservedRows: []string{"[1]"},
			Environment:     &RunEnvironment{Round: round, WarmupIterations: 20},
			RawPGXWaterfall: &PostgresBoundaryWaterfall{WarmupIterations: 20, MeasurementOrder: productionOrder},
			PostgresReferences: []PostgresReferenceResult{{
				Name: "s3_unidirectional_trail_cte", Architecture: "SP-S3-U-D",
				FullComparator: true, SemanticValidation: "exact_public_observation",
				MeasurementOrder: referenceOrder,
				RowCount:         1, ObservedRows: []string{"[1]"}, Stats: DurationStats{WarmupIterations: 20},
			}},
		}
		for iteration := 1; iteration <= samples; iteration++ {
			record.RawPGXWaterfall.Samples = append(record.RawPGXWaterfall.Samples, BoundarySample{Iteration: iteration, Total: productionDuration, Rows: 1})
			record.PostgresReferences[0].Stats.Samples = append(record.PostgresReferences[0].Stats.Samples, LatencySample{
				Round: round, Iteration: iteration, Classification: "warm", Duration: referenceDuration,
			})
		}
		records = append(records, record)
	}
	return records
}
