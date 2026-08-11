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

func TestBuildReferencePairReportComparesExactMatchedArms(t *testing.T) {
	records := make([]CaseResult, 0, 10)
	for round := 1; round <= 10; round++ {
		baselineOrder, candidateOrder := 2, 3
		if round%2 == 0 {
			baselineOrder, candidateOrder = 3, 2
		}
		record := CaseResult{
			Dataset:       "fixture",
			Name:          "distance",
			ExecutionMode: ModePostgresSQL,
			Status:        StatusOK,
			RowCount:      1,
			ObservedRows:  []string{"[2]"},
			Environment: &RunEnvironment{
				Round:            round,
				WarmupIterations: 20,
			},
			PostgresReferences: []PostgresReferenceResult{
				{
					Name:               "s3",
					Architecture:       "SP-S3-U-D",
					FullComparator:     true,
					SemanticValidation: "exact_public_observation",
					RowCount:           1,
					ObservedRows:       []string{"[2]"},
					MeasurementOrder:   baselineOrder,
					Stats: DurationStats{
						WarmupIterations: 20,
					},
				},
				{
					Name:               "s1",
					Architecture:       "SP-S1",
					FullComparator:     true,
					SemanticValidation: "exact_public_observation",
					RowCount:           1,
					ObservedRows:       []string{"[2]"},
					MeasurementOrder:   candidateOrder,
					Stats: DurationStats{
						WarmupIterations: 20,
					},
				},
			},
		}
		stampReferencePairIdentity(&record)
		for iteration := 1; iteration <= 50; iteration++ {
			record.PostgresReferences[0].Stats.Samples = append(record.PostgresReferences[0].Stats.Samples, LatencySample{
				Round:          round,
				Iteration:      iteration,
				Classification: "warm",
				Duration:       time.Millisecond,
			})
			record.PostgresReferences[1].Stats.Samples = append(record.PostgresReferences[1].Stats.Samples, LatencySample{
				Round:          round,
				Iteration:      iteration,
				Classification: "warm",
				Duration:       2 * time.Millisecond,
			})
		}
		records = append(records, record)
	}

	report, err := buildReferencePairReport(records, ReferencePairOptions{
		Seed:           1,
		Confidence:     0.975,
		BootstrapCount: 100,
		BaselineName:   "s3",
		CandidateName:  "s1",
	})
	require.NoError(t, err)
	require.Len(t, report.Cases, 1)
	require.Equal(t, 10, report.Cases[0].Rounds)
	require.InDelta(t, 2, report.Cases[0].MedianRatio.Estimate, 0.0001)
	require.InDelta(t, 2, report.Cases[0].P95Ratio.Estimate, 0.0001)
	require.Equal(t, time.Millisecond, report.Cases[0].MedianChange.Estimate)
}

func TestBuildReferencePairReportComparesValidatedHydrationBoundaries(t *testing.T) {
	records := make([]CaseResult, 0, 10)
	for round := 1; round <= 10; round++ {
		baselineOrder, candidateOrder := 2, 3
		if round%2 == 0 {
			baselineOrder, candidateOrder = 3, 2
		}
		record := CaseResult{
			Dataset:       "fixture",
			Name:          "path",
			ExecutionMode: ModePostgresSQL,
			Status:        StatusOK,
			RowCount:      1,
			ObservedRows:  []string{"[path]"},
			Environment: &RunEnvironment{
				Round:            round,
				WarmupIterations: 20,
			},
			PostgresReferences: []PostgresReferenceResult{
				{
					Name:               "m0",
					Architecture:       "MAT-M0",
					Boundary:           "edge IDs",
					SemanticValidation: "precomputed_exact_path_inputs",
					RowCount:           1,
					ObservedRows:       []string{"[path]"},
					MeasurementOrder:   baselineOrder,
					Stats: DurationStats{
						WarmupIterations: 20,
					},
				},
				{
					Name:               "m1",
					Architecture:       "MAT-M1",
					Boundary:           "node and edge IDs",
					SemanticValidation: "precomputed_exact_path_inputs",
					RowCount:           1,
					ObservedRows:       []string{"[path]"},
					MeasurementOrder:   candidateOrder,
					Stats: DurationStats{
						WarmupIterations: 20,
					},
				},
			},
		}
		stampReferencePairIdentity(&record)
		for iteration := 1; iteration <= 50; iteration++ {
			record.PostgresReferences[0].Stats.Samples = append(record.PostgresReferences[0].Stats.Samples, LatencySample{
				Round:          round,
				Iteration:      iteration,
				Classification: "warm",
				Duration:       time.Millisecond,
			})
			record.PostgresReferences[1].Stats.Samples = append(record.PostgresReferences[1].Stats.Samples, LatencySample{
				Round:          round,
				Iteration:      iteration,
				Classification: "warm",
				Duration:       2 * time.Millisecond,
			})
		}
		records = append(records, record)
	}

	report, err := buildReferencePairReport(records, ReferencePairOptions{
		Seed:           1,
		Confidence:     0.975,
		BootstrapCount: 100,
		BaselineName:   "m0",
		CandidateName:  "m1",
	})
	require.NoError(t, err)
	require.Len(t, report.Cases, 1)
	require.Equal(t, "precomputed_exact_path_inputs", report.Cases[0].BaselineSemanticValidation)
	require.Equal(t, "edge IDs", report.Cases[0].BaselineBoundary)
	require.InDelta(t, 2, report.Cases[0].MedianRatio.Estimate, 0.0001)
	require.InDelta(t, 2, report.Cases[0].P95Ratio.Estimate, 0.0001)
}

func TestBuildReferencePairReportRejectsMixedExactBoundaries(t *testing.T) {
	record := CaseResult{
		Dataset:       "fixture",
		Name:          "path",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
		RowCount:      1,
		ObservedRows:  []string{"[path]"},
		Environment: &RunEnvironment{
			Round:            1,
			WarmupIterations: 20,
		},
		PostgresReferences: []PostgresReferenceResult{
			{
				Name:               "full",
				FullComparator:     true,
				SemanticValidation: "exact_public_observation",
				RowCount:           1,
				ObservedRows:       []string{"[path]"},
				MeasurementOrder:   2,
				Stats: DurationStats{
					WarmupIterations: 20,
				},
			},
			{
				Name:               "hydration",
				SemanticValidation: "precomputed_exact_path_inputs",
				RowCount:           1,
				ObservedRows:       []string{"[path]"},
				MeasurementOrder:   3,
				Stats: DurationStats{
					WarmupIterations: 20,
				},
			},
		},
	}

	_, err := buildReferencePairReport([]CaseResult{record}, ReferencePairOptions{
		Seed:          1,
		Confidence:    0.975,
		BaselineName:  "full",
		CandidateName: "hydration",
	})
	require.ErrorContains(t, err, "does not share an exact comparable boundary")
}

func TestBuildReferencePairReportSupportsLabeledOrderedIDDiscovery(t *testing.T) {
	records := make([]CaseResult, 0, 5)
	for round := 1; round <= 5; round++ {
		baselineOrder, candidateOrder := 2, 3
		if round%2 == 0 {
			baselineOrder, candidateOrder = 3, 2
		}
		record := CaseResult{
			Dataset:       "fixture",
			Name:          "ordered",
			ExecutionMode: ModePostgresSQL,
			Status:        StatusOK,
			RowCount:      1,
			ObservedRows:  []string{"[public]"},
			Environment: &RunEnvironment{
				Round:            round,
				WarmupIterations: 5,
			},
			PostgresReferences: []PostgresReferenceResult{
				{
					Name:               "search_ordered_ids",
					Architecture:       "EXPANSION-STEPWISE-FORWARD",
					ObservationShape:   "ordered_ids",
					SemanticValidation: "exact_ordered_ids",
					RowCount:           1,
					ObservedRows:       []string{"[[1,2],3,[4]]"},
					MeasurementOrder:   baselineOrder,
					Stats: DurationStats{
						WarmupIterations: 5,
					},
				},
				{
					Name:               "suffix_seeded_reverse_ordered_ids",
					Architecture:       "EXPANSION-SUFFIX-SEEDED-REVERSE",
					ObservationShape:   "ordered_ids",
					SemanticValidation: "exact_ordered_ids",
					RowCount:           1,
					ObservedRows:       []string{"[[1,2],3,[4]]"},
					MeasurementOrder:   candidateOrder,
					Stats: DurationStats{
						WarmupIterations: 5,
					},
				},
			},
		}
		stampReferencePairIdentity(&record)
		for iteration := 1; iteration <= 10; iteration++ {
			record.PostgresReferences[0].Stats.Samples = append(record.PostgresReferences[0].Stats.Samples, LatencySample{
				Round:          round,
				Iteration:      iteration,
				Classification: "warm",
				Duration:       2 * time.Millisecond,
			})
			record.PostgresReferences[1].Stats.Samples = append(record.PostgresReferences[1].Stats.Samples, LatencySample{
				Round:          round,
				Iteration:      iteration,
				Classification: "warm",
				Duration:       time.Millisecond,
			})
		}
		records = append(records, record)
	}

	report, err := buildReferencePairReport(records, ReferencePairOptions{
		Seed:           1,
		Confidence:     0.975,
		BootstrapCount: 100,
		BaselineName:   "search_ordered_ids",
		CandidateName:  "suffix_seeded_reverse_ordered_ids",
		Protocol:       referencePairProtocolDiscovery,
	})
	require.NoError(t, err)
	require.Equal(t, referencePairProtocolDiscovery, report.Protocol)
	require.Equal(t, 5, report.MinimumWarmups)
	require.Equal(t, 5, report.MinimumRounds)
	require.Equal(t, 10, report.MinimumSamples)
	require.Len(t, report.Cases, 1)
	require.InDelta(t, 0.5, report.Cases[0].MedianRatio.Estimate, 0.0001)
}

func TestBuildReferencePairReportRejectsChangedImplementationIdentity(t *testing.T) {
	records := make([]CaseResult, 0, 10)
	for round := 1; round <= 10; round++ {
		records = append(records, referencePairProtocolRecord(round, round%2 == 1))
	}
	records[4].PostgresReferences[0].ImplementationID = "changed"
	_, err := buildReferencePairReport(records, ReferencePairOptions{
		Confidence:    0.975,
		BaselineName:  "baseline",
		CandidateName: "candidate",
	})
	require.ErrorContains(t, err, "identity changed")
}

func TestBuildReferencePairReportRejectsUnbalancedArmOrder(t *testing.T) {
	records := make([]CaseResult, 0, 10)
	for round := 1; round <= 10; round++ {
		records = append(records, referencePairProtocolRecord(round, true))
	}
	_, err := buildReferencePairReport(records, ReferencePairOptions{
		Confidence:    0.975,
		BaselineName:  "baseline",
		CandidateName: "candidate",
	})
	require.ErrorContains(t, err, "does not alternate")
}

func referencePairProtocolRecord(round int, baselineFirst bool) CaseResult {
	baselineOrder, candidateOrder := 2, 3
	if !baselineFirst {
		baselineOrder, candidateOrder = candidateOrder, baselineOrder
	}
	record := CaseResult{
		Dataset:       "fixture",
		Name:          "protocol",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
		RowCount:      1,
		ObservedRows:  []string{"[1]"},
		Environment:   &RunEnvironment{Round: round, WarmupIterations: 20},
		PostgresReferences: []PostgresReferenceResult{
			{Name: "baseline", Architecture: "A", FullComparator: true, SemanticValidation: "exact_public_observation", RowCount: 1, ObservedRows: []string{"[1]"}, MeasurementOrder: baselineOrder, Stats: DurationStats{WarmupIterations: 20}},
			{Name: "candidate", Architecture: "B", FullComparator: true, SemanticValidation: "exact_public_observation", RowCount: 1, ObservedRows: []string{"[1]"}, MeasurementOrder: candidateOrder, Stats: DurationStats{WarmupIterations: 20}},
		},
	}
	stampReferencePairIdentity(&record)
	for iteration := 1; iteration <= 50; iteration++ {
		record.PostgresReferences[0].Stats.Samples = append(record.PostgresReferences[0].Stats.Samples, LatencySample{Round: round, Iteration: iteration, Classification: "warm", Duration: time.Millisecond})
		record.PostgresReferences[1].Stats.Samples = append(record.PostgresReferences[1].Stats.Samples, LatencySample{Round: round, Iteration: iteration, Classification: "warm", Duration: 2 * time.Millisecond})
	}
	return record
}

func stampReferencePairIdentity(record *CaseResult) {
	record.Environment.BinarySHA256 = "binary"
	record.Environment.DirtyDiffSHA256 = "dirty"
	record.Environment.SourceCommit = "commit"
	record.Environment.GOOS = "linux"
	record.Environment.GOARCH = "amd64"
	for idx := range record.PostgresReferences {
		reference := &record.PostgresReferences[idx]
		reference.ImplementationID = reference.Name + "-implementation"
		reference.SQLFingerprint = reference.Name + "-sql"
	}
}

func TestBuildReferencePairReportRejectsMismatchedOrderedIDObservations(t *testing.T) {
	record := CaseResult{
		Dataset:       "fixture",
		Name:          "ordered",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
		Environment: &RunEnvironment{
			Round:            1,
			WarmupIterations: 5,
		},
		PostgresReferences: []PostgresReferenceResult{
			{
				Name:               "search_ordered_ids",
				ObservationShape:   "ordered_ids",
				SemanticValidation: "exact_ordered_ids",
				RowCount:           1,
				ObservedRows:       []string{"[a]"},
				MeasurementOrder:   2,
				Stats: DurationStats{
					WarmupIterations: 5,
				},
			},
			{
				Name:               "suffix_seeded_reverse_ordered_ids",
				ObservationShape:   "ordered_ids",
				SemanticValidation: "exact_ordered_ids",
				RowCount:           1,
				ObservedRows:       []string{"[b]"},
				MeasurementOrder:   3,
				Stats: DurationStats{
					WarmupIterations: 5,
				},
			},
		},
	}

	_, err := buildReferencePairReport([]CaseResult{record}, ReferencePairOptions{
		Seed:          1,
		Confidence:    0.975,
		BaselineName:  "search_ordered_ids",
		CandidateName: "suffix_seeded_reverse_ordered_ids",
		Protocol:      referencePairProtocolDiscovery,
	})
	require.ErrorContains(t, err, "ordered-ID reference-pair observations differ")
}
