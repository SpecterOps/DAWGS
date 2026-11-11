// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
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

// TestBuildReferenceClosureReportPassesRatioOrResolution verifies that a small absolute gap within measurement resolution passes even when production is five percent slower.
func TestBuildReferenceClosureReportPassesRatioOrResolution(t *testing.T) {
	records := referenceClosureRecords(10, 50, time.Millisecond, 1050*time.Microsecond)
	report, err := buildReferenceClosureReport(records, ReferenceClosureOptions{
		Seed:           7,
		Confidence:     0.975,
		BootstrapCount: 250,
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

// TestBuildReferenceClosureReportUsesCaseAAResolution verifies that observed production-side A/A noise raises the per-case absolute resolution above the default floor.
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
		Seed:           1,
		Confidence:     0.975,
		BootstrapCount: 100,
	})

	require.NoError(t, err)
	require.True(t, report.Passed)
	require.Greater(t, report.Cases[0].ProductionAAResolution, 100*time.Microsecond)
	require.Equal(t, report.Cases[0].ProductionAAResolution, report.Cases[0].AbsoluteResolution)
}

// TestBuildReferenceClosureReportFailsMaterialGap verifies that a confidence interval exceeding both ratio and absolute-resolution allowances fails closure.
func TestBuildReferenceClosureReportFailsMaterialGap(t *testing.T) {
	records := referenceClosureRecords(10, 50, time.Millisecond, 1500*time.Microsecond)
	report, err := buildReferenceClosureReport(records, ReferenceClosureOptions{
		Seed:           1,
		Confidence:     0.975,
		BootstrapCount: 100,
	})

	require.NoError(t, err)
	require.False(t, report.Passed)
	require.ErrorContains(t, reasonsError(report.Cases[0].Reasons), "ratio upper")
}

// TestBuildReferenceClosureReportEnforcesProtocolAndExactComparator verifies minimum rounds/samples, exact public observations, and carryover-balanced measurement order.
func TestBuildReferenceClosureReportEnforcesProtocolAndExactComparator(t *testing.T) {
	records := referenceClosureRecords(9, 49, time.Millisecond, time.Millisecond)
	report, err := buildReferenceClosureReport(records, ReferenceClosureOptions{
		Seed:           1,
		Confidence:     0.975,
		BootstrapCount: 100,
	})
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.ErrorContains(t, reasonsError(report.Cases[0].Reasons), "10-20 matched rounds")
	require.ErrorContains(t, reasonsError(report.Cases[0].Reasons), "at least 50 samples")

	records = referenceClosureRecords(10, 50, time.Millisecond, time.Millisecond)
	records[0].PostgresReferences[0].ObservedRows = []string{"[2]"}
	_, err = buildReferenceClosureReport(records, ReferenceClosureOptions{
		Seed:       1,
		Confidence: 0.975,
	})
	require.ErrorContains(t, err, "observation differs")

	records = referenceClosureRecords(10, 50, time.Millisecond, time.Millisecond)
	records[1].PostgresReferences[0].MeasurementOrder = 2
	_, err = buildReferenceClosureReport(records, ReferenceClosureOptions{
		Seed:       1,
		Confidence: 0.975,
	})
	require.ErrorContains(t, err, "lacks carryover-balanced")
}

func TestBuildReferenceClosureReportBindsCandidateSourceWorkloadAndQuery(t *testing.T) {
	records := referenceClosureRecords(10, 50, time.Millisecond, time.Millisecond)
	report, err := buildReferenceClosureReport(records, ReferenceClosureOptions{
		Seed: 1, Confidence: defaultConfidenceLevel, BootstrapCount: defaultBootstrapCount,
	})
	require.NoError(t, err)
	require.Equal(t, referenceClosureReportVersion, report.Version)
	require.Equal(t, "candidate", report.Candidate)
	require.Equal(t, "deadbeef", report.SourceCommit)
	require.Equal(t, cleanWorkingTreeSHA256(), report.DirtyDiffSHA256)
	require.Equal(t, strings.Repeat("a", 64), report.BinarySHA256)
	require.Equal(t, strings.Repeat("a", 64), report.CorpusSHA256)
	require.Equal(t, defaultBootstrapCount, report.BootstrapCount)
	require.Len(t, report.Cases, 1)
	require.Equal(t, "training", report.Cases[0].QualificationSplit)
	require.Equal(t, strings.Repeat("a", 64), report.Cases[0].WorkloadSHA256)
	require.True(t, lowercaseSHA256(report.Cases[0].QuerySHA256))
	require.Len(t, report.Cases[0].ProductionRuntimeReceiptChains, 500)
}

func TestBuildReferenceClosureReportRejectsIdentityDrift(t *testing.T) {
	tests := map[string]struct {
		mutate func([]CaseResult)
		reason string
	}{
		"source": {
			mutate: func(records []CaseResult) { records[1].Environment.SourceCommit = "other" },
			reason: "source, binary, or corpus identity changed",
		},
		"candidate": {
			mutate: func(records []CaseResult) { records[1].Optimization.TargetOutcomes[0].Applied = "other" },
			reason: "production executor identity changed",
		},
		"workload": {
			mutate: func(records []CaseResult) { records[1].WorkloadSHA256 = strings.Repeat("b", 64) },
			reason: "workload, query, or split identity changed",
		},
		"query": {
			mutate: func(records []CaseResult) { records[1].Cypher += " limit 1" },
			reason: "workload, query, or split identity changed",
		},
		"sql fingerprint": {
			mutate: func(records []CaseResult) { records[1].SQLFingerprint = strings.Repeat("b", 64) },
			reason: "lacks exact workload and translated-SQL identity",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			records := referenceClosureRecords(10, 50, time.Millisecond, time.Millisecond)
			test.mutate(records)
			_, err := buildReferenceClosureReport(records, ReferenceClosureOptions{Seed: 1, Confidence: defaultConfidenceLevel})
			require.ErrorContains(t, err, test.reason)
		})
	}
}

// referenceClosureRecords returns carryover-balanced production/reference rounds with exact observations and uniform warm timings.
func referenceClosureRecords(rounds, samples int, referenceDuration, productionDuration time.Duration) []CaseResult {
	records := make([]CaseResult, 0, rounds)
	digest := strings.Repeat("a", 64)
	cypherQuery := "match p = shortestPath((a)-[:Edge*1..4]->(b)) return length(p)"
	sqlQuery := "select 1"
	for round := 1; round <= rounds; round++ {
		productionOrder, referenceOrder := referenceClosureMeasurementOrder(true, round)
		record := CaseResult{
			Dataset:        "fixture",
			Name:           "distance",
			ExecutionMode:  ModePostgresSQL,
			Status:         StatusOK,
			RowCount:       1,
			ObservedRows:   []string{"[1]"},
			WorkloadSHA256: digest,
			Cypher:         cypherQuery,
			SQL:            sqlQuery,
			SQLFingerprint: sqlFingerprint(sqlQuery),
			Shape:          WorkloadShape{QualificationSplit: "training"},
			Environment: &RunEnvironment{
				ArtifactSchemaVersion: 2,
				CorpusSHA256:          digest,
				SourceCommit:          "deadbeef",
				DirtyDiffSHA256:       cleanWorkingTreeSHA256(),
				BinarySHA256:          digest,
				Round:                 round,
				WarmupIterations:      20,
			},
			RawPGXWaterfall: &PostgresBoundaryWaterfall{
				WarmupIterations: 20,
				MeasurementOrder: productionOrder,
				SQLFingerprint:   sqlFingerprint(sqlQuery),
			},
			Optimization: &translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{{
				Family: "SP", Applied: "candidate",
			}}},
			PostgresReferences: []PostgresReferenceResult{{
				Name:               "s3_unidirectional_trail_cte",
				Architecture:       "SP-S3-U-D",
				FullComparator:     true,
				SemanticValidation: "exact_public_observation",
				MeasurementOrder:   referenceOrder,
				RowCount:           1,
				ObservedRows:       []string{"[1]"},
				Stats: DurationStats{
					WarmupIterations: 20,
				},
			}},
		}
		for iteration := 1; iteration <= samples; iteration++ {
			record.RawPGXWaterfall.Samples = append(record.RawPGXWaterfall.Samples, BoundarySample{
				Iteration: iteration,
				Total:     productionDuration,
				Rows:      1,
			})
			record.PostgresReferences[0].Stats.Samples = append(record.PostgresReferences[0].Stats.Samples, LatencySample{
				Round:          round,
				Iteration:      iteration,
				Classification: "warm",
				Duration:       referenceDuration,
			})
			invocation := fmt.Sprintf("closure-%d-%d", round, iteration)
			fallback := false
			record.Stats.Samples = append(record.Stats.Samples, LatencySample{
				Round: round, Iteration: iteration, Classification: "warm", Duration: productionDuration,
				RuntimeInvocationID: invocation, RuntimeIdentity: "candidate", RuntimeBranch: "selected", FallbackExecuted: &fallback,
				RuntimeReceiptEvents: []RuntimeReceiptEvent{{InvocationID: invocation, Ordinal: 1, RuntimeIdentity: "candidate", RuntimeBranch: "selected"}},
			})
		}
		records = append(records, record)
	}
	return records
}
